#
# Copyright (c) 2026, Nikolai Shakin
#
# SPDX-License-Identifier: BSD-2-Clause
#

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
)

from .flow_controller import FlowController
from ..serializer.serializer import AsteriskFrameSerializer, AsteriskCommandFrame


class AsteriskWebsocketOutputTransport(FastAPIWebsocketOutputTransport):
    """Subclass of FastAPIWebsocketOutputTransport to handle Asterisk WebSocket channel communication."""

    def __init__(
        self,
        transport: "AsteriskWebsocketTransport",
        client: FastAPIWebsocketClient,
        params: FastAPIWebsocketParams | None = None,
        name: str | None = None,
    ):
        if params is None:
            params = FastAPIWebsocketParams(
                serializer=AsteriskFrameSerializer(),
                audio_in_enabled=True,
                audio_out_enabled=True,
            )
        super().__init__(transport, client, params)
        self._flow_controller = None

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
        if self._client.is_closing or not self._client.is_connected:
            logger.warning(
                f"Cannot send START_MEDIA_BUFFERING command because the WebSocket client is closing or already closed."
            )
            return
        if not self._params.serializer:
            logger.error(
                f"Cannot send START_MEDIA_BUFFERING command because no serializer is set in the transport parameters."
            )
            return
        try:
            cmd = await self._params.serializer.serialize(
                AsteriskCommandFrame("START_MEDIA_BUFFERING")
            )
            if cmd:
                await self._client.send(cmd)
                logger.info(
                    f"Sent START_MEDIA_BUFFERING command to Asterisk WebSocket channel to enable audio buffering."
                )
        except Exception as e:
            logger.error(
                f"{self} exception sending START_MEDIA_BUFFERING: {e.__class__.__name__} ({e})"
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
        elif isinstance(frame, AsteriskCommandFrame):
            # Send the command to Asterisk WebSocket channel
            if self._client.is_closing or not self._client.is_connected:
                logger.warning(
                    f"Cannot send AsteriskCommandFrame because the WebSocket client is closing or already closed."
                )
                return
            if not self._params.serializer:
                logger.error(
                    f"Cannot send AsteriskCommandFrame because no serializer is set in the transport parameters."
                )
                return
            try:
                cmd = await self._params.serializer.serialize(frame)
                if cmd:
                    await self._client.send(cmd)
                    logger.info(
                        f"Sent command: {frame.cmd} to Asterisk WebSocket channel."
                    )
            except Exception as e:
                logger.error(
                    f"{self} exception sending AsteriskCommandFrame: {e.__class__.__name__} ({e})"
                )
    async def write_audio_frame(self, frame: OutputAudioRawFrame) -> bool:
        """Write an audio frame into local buffer.

        The method overrides parent class method. Effectively the audio frame is passed to the flow controller
        instead of writing them directly to the websocket. Formally, this method doesn't write audio frames as the name suggests.

        Args:
            frame: The output audio frame to write.

        Returns:
            True if the audio frame was "written" (passed to the flow controller) successfully, False otherwise.
        """

        if self._client.is_closing or not self._client.is_connected:
            logger.warning(
                f"Cannot write audio frame because the WebSocket client is closing or already closed."
            )
            return False

        if not self._params.serializer:
            logger.error(
                f"Serializer is not set in transport parameters. Cannot write audio frame."
            )
            return False

        if self._flow_controller is None:
            logger.error(
                f"Flow controller is not initialized. Cannot write audio frame."
            )
            return False

        frame = OutputAudioRawFrame(
            audio=frame.audio,
            sample_rate=frame.sample_rate,
            num_channels=frame.num_channels,
        )

        try:
            payload = await self._params.serializer.serialize(frame)
            if payload:
                if type(payload) == bytes:
                    self._flow_controller(payload)
                    return True
                else:
                    logger.error(
                        f"Serialized audio frame is not bytes. Got {type(payload)} instead. Cannot write audio frame."
                    )
                    return False
            else:
                logger.trace(
                    f"Serializer returned None or empty payload. Cannot write audio frame."
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
