#
# Copyright (c) 2026, Nikolai Shakin
#
# SPDX-License-Identifier: BSD-2-Clause
#
import asyncio

from fastapi import WebSocket
from loguru import logger
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketClient,
    FastAPIWebsocketOutputTransport,
    FastAPIWebsocketTransport,
    FastAPIWebsocketParams,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.frames.frames import (
    Frame,
    InterruptionFrame,
    CancelFrame,
    StopFrame,
    InputTransportMessageFrame,
    OutputAudioRawFrame,
    TTSStoppedFrame,
)

from .flow_controller import FlowController
from ..serializer.serializer import AsteriskFrameSerializer
from ..frames import AsteriskCommandFrame

class AsteriskWebsocketOutputTransport(FastAPIWebsocketOutputTransport):
    """Subclass of FastAPIWebsocketOutputTransport to handle Asterisk WebSocket channel communication."""

    def __init__(
        self,
        transport: "AsteriskWebsocketTransport",
        client: FastAPIWebsocketClient,
        params: FastAPIWebsocketParams | None = None,
        **kwargs,
    ):
        if params is None:
            params = FastAPIWebsocketParams(
                serializer=AsteriskFrameSerializer(),
                audio_in_enabled=True,
                audio_out_enabled=True,
            )
        super().__init__(transport, client, params, **kwargs)
        self._flow_controller = None
        self._debug_asterisk_status = kwargs.get('debug_asterisk_status', False)

    async def serialize(self, frame: Frame) -> bytes | str | None:
        """Serialize a frame to bytes using the serializer in transport parameters."""
        if not self._params.serializer:
            logger.error(f"Cannot serialize the frame {type(frame)} because no serializer is set in transport parameters.")
            return None
        try:
            return await self._params.serializer.serialize(frame)
        except Exception as e:
            logger.error(f"{self} exception serializing frame: {e.__class__.__name__} ({e})")
            return None

    async def _media_start_handler(self, frame: InputTransportMessageFrame):
        """Handle the MEDIA_START event.

        Initializes the flow controller with ptime and psize values from the MEDIA_START event data.
        Sends a START_MEDIA_BUFFERING command to Asterisk to enable audio buffering.
        """

        ptime = int(frame.message.get("ptime", 0))
        psize = int(frame.message.get("optimal_frame_size", 0))

        if ptime <= 0 or psize <= 0:
            logger.error(
                f"Invalid ptime ({ptime}) or psize ({psize}) in MEDIA_START event {frame.message}. Cannot initialize flow controller."
            )
            return

        self._flow_controller = FlowController(ptime, psize, self._client)

        logger.debug(
            f"Initialized flow controller with ptime={ptime} ms, psize={psize} bytes. Remote buffer low water mark: {self._flow_controller._remote_buffer_low_water} bytes, high water mark: {self._flow_controller._remote_buffer_high_water} bytes."
        )

        # Send START_MEDIA_BUFFERING command to Asterisk WebSocket channel to enable audio buffering on the Asterisk side
        cmd_frame =  AsteriskCommandFrame("START_MEDIA_BUFFERING")
        await self.send_asterisk_command(cmd_frame)

    async def _monitor_status(self):
        """Monitor the status of the Asterisk WebSocket channel.
        
        Periodically sends a GET_STATUS command to Asterisk. In the response, Asterisk will send STATUS events.
        It's only used for debugging, and doesn't affect the logic.
        """
        
        RETRY_INTERVAL = 0.08  # seconds

        cmd_frame = AsteriskCommandFrame("GET_STATUS")
        while True:
            await self.send_asterisk_command(cmd_frame)
            await asyncio.sleep(RETRY_INTERVAL)

    async def send_asterisk_command(self, frame: AsteriskCommandFrame) -> None:
        """Send an AsteriskCommandFrame to the Asterisk WebSocket channel."""
        try:
            cmd = await self.serialize(frame)
            if cmd:
                await self._client.send(cmd)
                logger.info(
                    f"Sent command: {frame.cmd} to Asterisk WebSocket channel."
                )
        except Exception as e:
            logger.error(
                f"{self} exception sending AsteriskCommandFrame: {e.__class__.__name__} ({e})"
            )

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Process outgoing frames.

        Args:
            frame: The frame to process.
            direction: The direction of frame flow in the pipeline.
        """
        await super().process_frame(frame, direction)

        if isinstance(frame, (InterruptionFrame, CancelFrame, StopFrame)):
            # Drop any buffered audio in local and remote buffers to avoid replaying stale PCM
            if self._flow_controller:
                self._flow_controller.drop_buffer()
        elif (
            isinstance(frame, InputTransportMessageFrame)
            and frame.message.get("event", None) == "MEDIA_START"
        ):
            await self._media_start_handler(frame)
            if self._debug_asterisk_status:
                asyncio.create_task(self._monitor_status())

        elif isinstance(frame, AsteriskCommandFrame):
            await self.send_asterisk_command(frame)
                    
    async def push_frame(self, frame: Frame, direction: FrameDirection = FrameDirection.DOWNSTREAM):
        # Catch bot-stopped speaking frames and hold them until the bot has effectively stopped speaking (buffer on the Asterisk side is empty)
        if isinstance(frame, TTSStoppedFrame):
            if self._flow_controller and not self._flow_controller.bot_stopped_speaking_fence.is_set():
                logger.trace("BotStoppedSpeakingFrame is on hold until the bot has effectively stopped speaking.")
                # Hold them
                await self._flow_controller.bot_stopped_speaking_fence.wait()
                logger.debug("Bot effectively stopped speaking. Sending BotStoppedSpeakingFrame to the pipeline.")
        await super().push_frame(frame)
    
    async def write_audio_frame(self, frame: OutputAudioRawFrame) -> bool:
        """Write an audio frame into local buffer.

        The method overrides parent class method. Effectively the audio frame is passed to the flow controller
        instead of writing them directly to the websocket. Formally, this method doesn't write audio frames as the name suggests.

        Args:
            frame: The output audio frame to write.

        Returns:
            True if the audio frame was "written" (passed to the flow controller) successfully, False otherwise.
        """
        frame = OutputAudioRawFrame(
            audio=frame.audio,
            sample_rate=frame.sample_rate,
            num_channels=frame.num_channels,
        )

        try:
            payload = await self.serialize(frame)
            if payload:
                if isinstance(payload, bytes):
                    if self._flow_controller is None:
                        logger.error(
                            "Flow controller is not initialized. Cannot write audio frame."
                        )
                        return False
                    self._flow_controller(payload)
                    return True
                else:
                    logger.error(
                        f"Serialized audio frame is not bytes. Got {type(payload)} instead. Cannot write audio frame."
                    )
                    return False
            else:
                logger.trace(
                    "Serializer returned None or empty payload. Cannot write audio frame."
                )
                return False
        except Exception as e:
            logger.error(f"{self} exception sending data: {e.__class__.__name__} ({e})")
            return False


class AsteriskWebsocketTransport(FastAPIWebsocketTransport):
    """Subclass of FastAPIWebsocketTransport to handle Asterisk WebSocket channel communication."""

    def __init__(
        self,
        websocket: WebSocket,
        params: FastAPIWebsocketParams | None = None,
        input_name: str | None = None,
        output_name: str | None = None,
    ):
        if params is None:
            params = FastAPIWebsocketParams(
                serializer=AsteriskFrameSerializer(),
                audio_in_enabled=True,
                audio_out_enabled=True,
            )
        super().__init__(websocket, params, input_name, output_name)

        self._output = AsteriskWebsocketOutputTransport(
            self, self._client, params, name=output_name
        )
