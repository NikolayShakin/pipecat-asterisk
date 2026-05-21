#
# Copyright (c) 2026, Nikolai Shakin
#
# SPDX-License-Identifier: BSD-2-Clause
#

import asyncio
from loguru import logger
import time
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketClient,
)


class FlowController:
    """Controls the flow of audio frames sent over the WebSocket connection.

    It manages a local buffer and estimates the utilization of the remote buffer
    on the Asterisk side to prevent buffer overflow and audio under-runs. Audio
    chunks are dispatched in batches based on configured low and high water marks,
    ensuring smooth playback while respecting WebSocket message size limits.
    """

    # Percentages of the remote buffer size to use as low and high water marks for flow control.
    REMOTE_BUFFER_LOW_WATER = 0.1
    REMOTE_BUFFER_HIGH_WATER = 0.8

    # Size of the remote buffer in frames(or audio chunks) of psize. It's currently (Apr 2026) hardcoded on Asterisk side.
    REMOTE_BUFFER_SIZE = 1000

    # Minimum number of bytes to send when the remote buffer is in working range (between low and high water marks).
    # We don't need to send data in small chunks in every tick if the buffer is loaded on the remote side.
    # 50 frames is about 1 second of audio at 20ms ptime, so after we reached the low water mark we will send audio in batches of at least one second of audio.
    # So with the default values we will have at least 20ms*1000*0.1 = 2 seconds of audio in the remote buffer before we start sending audio in batches,
    # and we will send at least 1 second of audio every time we send while the remote buffer is between 20% and 80% full.
    # If the remote buffer is above 80% full we will stop sending until it goes below 80% again.
    # If the remote buffer is below 20% full we will send whatever we have in the local buffer without waiting for the minimum batch size to be reached,
    # to quickly fill the remote buffer and avoid buffer under-utilization.
    MIN_BATCH = 50

    # Based on Asterisk documentation.The maximum websocket message size the underlying websocket code can handle is 65500 bytes.
    # We need to ensure we don't exceed this limit when sending audio chunks.
    MAX_WS_SEND = 65500

    def __init__(
        self, ptime: int, psize: int, websocket_client: FastAPIWebsocketClient
    ):
        self._ptime = ptime  # Audio chunk duration. In milliseconds
        self._psize = psize  # Audio chunk size. In bytes
        self._websocket_client = websocket_client
        self._local_buffer = bytearray()
        self._remote_buffer_low_water = (
            self.REMOTE_BUFFER_LOW_WATER * self.REMOTE_BUFFER_SIZE * self._psize
        )
        self._remote_buffer_high_water = (
            self.REMOTE_BUFFER_HIGH_WATER * self.REMOTE_BUFFER_SIZE * self._psize
        )
        self._remote_buffer_utilization = 0.0  # In bytes, but it has to be float otherwise it will drift badly due to integer division
        self._min_batch = self.MIN_BATCH * self._psize
        # Start the flow control task
        self._flow_control = asyncio.create_task(self.flow_control())

    def __call__(self, chunk: bytes) -> None:
        """Add an audio chunk to the local buffer

        It handles arbitrary sized audio chunks but it's expected that the audio chunks are passed properly sampled.
        No modifications are made on the audio chunks content after they are passed to the flow controller.

        Args:
            chunk: The audio chunk to add to the local buffer.

        """
        self._local_buffer.extend(chunk)
        logger.trace(
            f"Buffered {len(chunk)} bytes to local buffer. Local buffer size: {len(self._local_buffer)} bytes."
        )

    async def flow_control(self):
        """Keep track of the remote buffer utilization and send audio whenever possible.

        The method runs an infinite loop that:
            - Calculate the remote buffer utilization using monotonic time instead of async sleeping time to avoid drift.
            - Implement the flow control logic based on the remote buffer utilization and local buffer size.
            - Sends audio chunks whenever the remote buffer utilization is below the low water mark and there are audio chunks in the local buffer, but never exceed the high water mark or the websocket maximum message size.
        """

        last_tick = time.monotonic()
        while True:
            await asyncio.sleep(
                self._ptime / 1000
            )  # Sleep for the duration of one audio chunk
            current_time = time.monotonic()
            elapsed_time = current_time - last_tick
            self._remote_buffer_utilization = max(
                0,
                self._remote_buffer_utilization
                - (self._psize * 1000 / self._ptime) * elapsed_time,
            )
            last_tick = current_time

            # Flow control logic
            # First check if we have something in the local buffer
            if len(self._local_buffer) > 0:
                # If the remote buffer is under the low water mark we send whatever we have in the local buffer
                if self._remote_buffer_utilization < self._remote_buffer_low_water:
                    await self.send_chunks()

                # If the remote buffer is in working range (between the low and high water marks)
                # we  only send if we have more than _min_batch bytes of audio in the local buffer to avoid sending small chunks on every tick
                # and we have at least twice as much free space in the remote buffer as the minimum batch size to avoid overfilling the remote buffer and causing audio dropouts on the Asterisk side.
                elif (
                    self._remote_buffer_utilization < self._remote_buffer_high_water - self._min_batch * 2
                ) and (len(self._local_buffer) >= self._min_batch):
                    await self.send_chunks()
                # If the remote buffer is above the high water mark we don't send anything and wait for the next tick to see if the remote buffer utilization has decreased enough to send more audio

    async def send_chunks(self):
        """Send audio chunks from the local buffer to websocket (effectively to the remote buffer on the Asterisk side).

        The method:
            - Sends as much bytes from the local buffer as possible but not more than remote buffer high water mark and websocket maximum message size.
            - Updates the remote buffer utilization accordingly.
        """

        # Calculate the number of bytes to send
        bytes_to_send = min(
            len(self._local_buffer), self.MAX_WS_SEND
        )  # Ensure we don't exceed the websocket maximum message size
        if bytes_to_send > 0:
            # Take the bytes to send from the local buffer
            chunk = bytes(self._local_buffer[:bytes_to_send])
            del self._local_buffer[:bytes_to_send]

            # Send the chunk to the websocket
            await self._websocket_client.send(chunk)
            # Update the remote buffer utilization
            self._remote_buffer_utilization += len(chunk)
            logger.debug(
                f"Sent {len(chunk)} bytes to websocket. Remote buffer utilization: {self._remote_buffer_utilization:.0f} bytes, {self._remote_buffer_utilization / (self._psize * self.REMOTE_BUFFER_SIZE) * 100:.1f}%."
            )

    def close(self, gracefully: bool = False):
        """Cancel the flow control task and optionally wait for the local buffer to be sent before cancelling.

        Args:
            gracefully: If True, wait for the local buffer to be sent before cancelling the flow control
        """
        if self._flow_control:
            if gracefully:
                logger.info(
                    f"Gracefully closing flow controller. Waiting for local buffer to be sent..."
                )
                while len(self._local_buffer) > 0:
                    time.sleep(
                        self._ptime / 1000
                    )  # Sleep for the duration of one audio chunk to give the flow control loop time to send the remaining audio in the local buffer
            self._flow_control.cancel()

    def drop_buffer(self):
        """Drop any buffered audio in the local buffer and reset remote buffer utilization to zero.

        This is used when an interruption/stop/cancel frame is processed to avoid replaying stale audio.
        """
        self._local_buffer.clear()
        self._remote_buffer_utilization = 0.0
