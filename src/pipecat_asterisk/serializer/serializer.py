#
# Copyright (c) 2026, Nikolai Shakin
#
# SPDX-License-Identifier: BSD-2-Clause
#

from inspect import iscoroutinefunction
from typing import Awaitable, Callable, Optional
from loguru import logger
from pipecat.audio.dtmf.types import KeypadEntry
from pipecat.audio.utils import create_stream_resampler
from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    Frame,
    InputAudioRawFrame,
    InputDTMFFrame,
    InputTransportMessageFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    StartFrame,
)
from pipecat.serializers.base_serializer import FrameSerializer

from .protocol import AsteriskWSProtocol
from ..frames import AsteriskCommandFrame

def handler(event_name):
    def decorator(func):
        func._event_name = event_name
        return func
    return decorator

class AsteriskFrameSerializer(FrameSerializer):
    """Asterisk WebSocket Serializer: Serializer for Asterisk WebSocket channel.

    This serializer handles converting between Pipecat frames and Asterisk's WebSocket
    channel events/commands and binary audio data and vice versa.
        Asterisk to Pipecat:
            - when DTMF detected on Asterisk websocket channel we send InputDTMFFrame to Pipecat.
            - when MEDIA_START event is received we send InputTransportMessageFrame to Pipecat with the event message as the payload,
                so the pipeline can use the information provided in that event (e.g., channel variables) and also know when the media starts flowing.
            - when MEDIA_XOFF event is received we log a warning that Asterisk is asking us to pause sending media, normally it should not happen if the transport implements flow control correctly.
            - when MEDIA_XON event is received we log that Asterisk is ready to receive media again after a MEDIA_XOFF event, again it's for information, it's not used.
            - when QUEUE_DRAINED event is received we send InputTransportMessageFrame to Pipecat with the event message as the payload,
                so the pipeline can know when Asterisk finished processing all the queued media, which might be useful to know when Asterisk finished playing all the TTS audio.
            - when binary audio data is received on Asterisk websocket channel we convert it to InputAudioRawFrame and send to Pipecat, after resampling if needed.
        Pipecat to Asterisk:
            - when an EndFrame or CancelFrame is processed we send HANGUP to Asterisk websocket channel.
            - when an InterruptionFrame is processed we send FLUSH_MEDIA to Asterisk websocket channel.
            - when an OutputAudioRawFrame is processed we send the raw audio bytes to Asterisk websocket channel, after resampling if needed.

        Some of the event handlers are just placeholders for now, they log the received events, but they can be extended if needed.
        In case you need to add more Asterisk event handlers or Pipecat frame handlers, you can add them as methods of the class using the @handler decorator. 
        @handler(Frame) for Pipecat frame handlers and @handler("EVENT_NAME") for Asterisk event handlers. They will be automatically registered.
    """

    # Asterisk slin sample rates supported by default, you can check it on your Asterisk with CLI>'core show codecs audio'
    SUPPORTED_SAMPLE_RATES = [12, 16, 24, 32, 44, 48, 96, 192, 128]  # kHz

    def __init__(self, sample_rate: int = 0):
        """Initialize the Asterisk WebSocket Serializer.

        Args:
            sample_rate: Sample rate in kHz used by Asterisk, defaults to 0 (will be populated during setup or from MEDIA_START event).
        """

        self._asterisk_ws_proto = AsteriskWSProtocol()
        self._input_resampler = None  # Will be initialized if resampling is needed
        self._output_resampler = None  # Will be initialized if resampling is needed
        self._pipeline_in_sample_rate = 0  # What rate should we send to the pipeline (and STT-like processors). Will be populated during setup
        self._pipeline_out_sample_rate = 0  # What rate should we expect to receive from TTS-like processors. Will be populated during setup
        self._asterisk_sample_rate = int(
            sample_rate
        )  # What sample rate is used in Asterisk websocket channel. If 0, will be populated during setup or from MEDIA_START event

        self._sync_frame_handlers: dict[type, Callable[[Frame], str | bytes | None]] = {}
        self._async_frame_handlers: dict[type, Callable[[Frame], Awaitable[str | bytes | None]]] = {}
        self._sync_event_handlers: dict[str, Callable[[dict], Frame | None]] = {}
        self._async_event_handlers: dict[str, Callable[[dict], Awaitable[Frame | None]]] = {}

        # Binding event/frame handlers based on the "_event_name" attribute set by the @handler decorator
        for method_name in dir(self):
            method = getattr(self, method_name, None)
            if method is None:
                continue

            event_name = getattr(method, "_event_name", None)

            if event_name:
                if isinstance(event_name, str): # Asterisk event handler
                    if iscoroutinefunction(method):# Async event handler
                        self._async_event_handlers[event_name] = method
                    else: # Sync event handler
                        self._sync_event_handlers[event_name] = method
                elif issubclass(event_name, Frame): # Frame handler
                    if iscoroutinefunction(method):# Async frame handler
                        self._async_frame_handlers[event_name] = method
                    else: # Sync frame handler
                        self._sync_frame_handlers[event_name] = method
                else:
                    logger.warning(f"Invalid handler method {method_name}, event type should be str for Asterisk events or subclass of Frame for Pipecat frames, got {type(event_name)} instead.")

    async def _handle_event(self, message: dict) -> Frame | None: # Asterisk events handler
        """Dispatch an Asterisk event.

        Args:
            message: The event message as a dictionary.
        """

        message_type = message.get("event", None)
        if message_type is None:
            logger.warning(
                f"Received Asterisk WebSocket message without 'event' field: {message}"
            )
            return None

        # try to find sync handler first
        handler = self._sync_event_handlers.get(message_type, None)
        if handler is not None:
            return handler(message)

        # if sync handler is not found, try async handlers
        async_handler = self._async_event_handlers.get(message_type, None)
        if async_handler is not None:
            return await async_handler(message)

        # If no handler is found, log the event and return None
        logger.info(f"Received unhandled Asterisk WebSocket event: {message}")
        return None

    ### Asterisk Event handlers ###
    @handler("MEDIA_START")
    def _media_start(self, message: dict) -> Frame | None:
        """MEDIA_START event handler.

        MEDIA_START event is the first one we receive from Asterisk.
        There are a few potentially useful parameters provided by Asterisk in the MEDIA_START event message:
          connection_id: A UUID that will be set on the MEDIA_WEBSOCKET_CONNECTION_ID channel variable.
          channel: The channel name on Asterisk.
          channel_id: The channel's unique id on Asterisk.
          format: The audio format set on the channel.
          optimal_frame_size: The optimal frame size from Astersisk's perspective.
          ptime: The packet size in milliseconds.
          channel_variables: An object containing the variables currently set on the channel.
          The latest can be very handy for moving data from dialplan/channel variables to Pipecat.
          However, it's only available in JSON subprotocol, in plain-text subprotocol you will not have access to channel variables.
          So if you need channel variables make sure to use JSON subprotocol on Asterisk WebSocket channel.
        We send MEDIA_START event object to the pipeline as InputTransportMessageFrame, so the pipeline "knows" about the media parameters.

        Args:
            message: The dictionary representing of the MEDIA_START event message from Asterisk.
        """
        # Check if codec is slin
        format = message.get("format", "").strip().lower()
        if not format.startswith("slin"):
            # Some Pipecat transports do transcoding on the fly in Pipecat, but this one doesn't for two reasons:
            #   1. Pipecat AudioFrame has to be in slin format, and Asterisk can send all the flavors of slin out-of-the-box.
            #   2. Asterisk is way more efficient in transcoding, it makes no sense to send non-slin audio to Pipecat and transcoding it there.
            raise ValueError(
                f"Unsupported audio format in Asterisk MEDIA_START event: [{message.get('format')}], we only support slin format for now. Please use make sure that Asterisk channel is configured to use slin[12..192]."
            )

        # Check if sample rate is defined
        if self._asterisk_sample_rate == 0:
            sample_rate = format[4:]
            if sample_rate:
                if not sample_rate.isdigit():
                    raise ValueError(
                        f"Invalid sample rate in Asterisk MEDIA_START event: [{sample_rate}] kHz. Sample rate should be a number in kHz, e.g., 'slin16' for 16000 Hz sample rate."
                    )
                # Check if sample rate is in the supported list, if not raise an error
                sample_rate = int(sample_rate)
                if sample_rate not in self.SUPPORTED_SAMPLE_RATES:
                    raise ValueError(
                        f"Unsupported sample rate in Asterisk MEDIA_START event: [{sample_rate}] kHz. Supported sample rates for slin format are {self.SUPPORTED_SAMPLE_RATES} kHz."
                    )
            else:
                sample_rate = 8

            self._asterisk_sample_rate = sample_rate * 1000

        logger.info(f"Received MEDIA_START event from Asterisk: {message}")

        # Check if input resampling is needed
        if self._pipeline_in_sample_rate != self._asterisk_sample_rate:
            logger.warning(
                f"Asterisk sample rate: ({self._asterisk_sample_rate} Hz) != pipeline input sample rate ({self._pipeline_in_sample_rate} Hz). Please, try to avoid resampling when possible."
            )
            self._input_resampler = self.create_resampler("input")

        # Check if output resampling is needed
        if self._pipeline_out_sample_rate != self._asterisk_sample_rate:
            logger.warning(
                f"Asterisk sample rate: ({self._asterisk_sample_rate} Hz) != pipeline output sample rate ({self._pipeline_out_sample_rate} Hz). Please, try to avoid resampling when possible."
            )
            self._output_resampler = self.create_resampler("output")

        return InputTransportMessageFrame(message=message)

    @handler("MEDIA_XOFF")
    def _media_xoff(self, message: dict) -> Frame | None:
        """MEDIA_XOFF event handler.

        The Asterisk's websocket channel driver will send this event when the frame queue length reaches the high water (XOFF) level.
        Any media sent after this has a high probability of being dropped. We don't use them in our flow control implementation,
        but getting this message means that our flow control implementation failed to keep the remote buffer under the high water mark, so we log a warning about it.

        Args:
            message: The dictionary representing of the MEDIA_XOFF event message from Asterisk.
        """
        logger.error(
            f"Received MEDIA_XOFF event from Asterisk: {message}. Oops, we hit the high water mark, probably Asterisk will drop the following audio frames."
        )
        return None

    @handler("MEDIA_XON")
    def _media_xon(self, message: dict) -> Frame | None:
        """MEDIA_XON event handler.

        The Asterisk's websocket channel driver will send this event when the frame queue length drops below the low water (XON) level.
        The app can then resume sending media. Again, out transport implements flow control to avoid reaching this point
        and it doesn't rely on these events for implementing flow control.

        Args:
            message: The dictionary representing of the MEDIA_XON event message from Asterisk.
        """
        logger.debug(
            f"Received MEDIA_XON event from Asterisk: {message}. Asterisk audio buffer is ready to receive audio again."
        )
        return None
    
    @handler("DTMF_END")
    def _dtmf_end(self, message: dict) -> Frame | None:
        """DTMF_END event handler.

        Handles DTMF_END events from Asterisk and converts them to InputDTMFFrame.

        Args:
            message: The dictionary representing of the DTMF_END event message from Asterisk.

        Returns:
            An InputDTMFFrame if a valid DTMF digit is found, otherwise None.
        """
        digit = message.get("digit")
        if digit:
            try:
                return InputDTMFFrame(KeypadEntry(digit))
            except ValueError:
                # Handle case where string doesn't match any enum value
                logger.warning(f"Invalid DTMF digit received: {digit}")
                return None
        return None

    @handler("QUEUE_DRAINED")
    def _queue_drained(self, message: dict) -> Frame | None:
        # TODO: add REPORT_QUEUE_DRAINED support in the transport
        """QUEUE_DRAINED event handler.

        Handles QUEUE_DRAINED events from Asterisk. This event indicates that Asterisk has processed all the queued media.
        We will only receive this event if we requested it by sending "REPORT_QUEUE_DRAINED", and only once per one "REPORT_QUEUE_DRAINED".
        Effectively, this means that Asterisk stopped playing audio to the channel(bot stopped speaking), which might be good to know in Pipecat.
        You can send AsteriskCommandFrame with cmd="REPORT_QUEUE_DRAINED" to the pipeline at any time from your pipeline, after that you will receive QUEUE_DRAINED event.

        Args:
            message: The dictionary representing of the QUEUE_DRAINED event message from Asterisk.
        """
        logger.debug(
            f"Received QUEUE_DRAINED event from Asterisk: {message}. Asterisk has processed all the queued media."
        )
        return InputTransportMessageFrame(message=message)

    #### Pipecat Frame handlers ####

    @handler(OutputAudioRawFrame)
    async def _output_audio_raw_frame(
        self, frame: OutputAudioRawFrame
    ) -> Optional[bytes]:
        """OutputAudioRawFrame handler.

        This handler extracts raw audio bytes from the OutputAudioRawFrame, resamples it if needed, and returns the raw audio bytes to be sent to Asterisk WebSocket channel.

        Args:
            frame: The OutputAudioRawFrame to be processed.
        """

        data = frame.audio

        if not data or len(data) == 0:
            logger.debug("OutputAudioRawFrame contains no audio data to serialize.")
            return None

        if self._pipeline_out_sample_rate != frame.sample_rate:
            logger.warning(
                f"OutputAudioRawFrame sample rate ({frame.sample_rate} Hz) != pipeline output sample rate ({self._pipeline_out_sample_rate} Hz). We can't resample the audio frame properly."
            )

        if frame.num_channels != 1:
            logger.warning(
                f"OutputAudioRawFrame has {frame.num_channels} channels, but Asterisk WebSocket channel only supports mono audio. We can't send this audio frame to Asterisk."
            )
            return None

        if self._asterisk_sample_rate == self._pipeline_out_sample_rate:
            logger.trace("Forwarding audio frame without resampling.")
            return data
        else:
            if self._output_resampler is None:
                logger.warning(
                    "Resampling is required but output resampler is not initialized, we can't resample the audio."
                )
                return None
            else:
                logger.trace(
                    f"Resampling audio from {self._pipeline_out_sample_rate} Hz to Asterisk sample rate {self._asterisk_sample_rate} Hz before sending to Asterisk."
                )
                resampled_audio = await self._output_resampler(data)
                if resampled_audio is None or len(resampled_audio) == 0:
                    logger.trace("Resampled audio contains no data.")
                    return None
                return resampled_audio

    @handler(AsteriskCommandFrame)
    def _asterisk_command_frame(self, frame: AsteriskCommandFrame) -> str:
        """AsteriskCommandFrame handler.

        Returns properly formatted arbitrary command for Asterisk WebSocket channel when an AsteriskCommandFrame is processed, using the command string provided in the frame.

        Args:
            frame: The AsteriskCommandFrame to be processed.
        """
        return self._asterisk_ws_proto.build(frame.cmd)

    @handler(EndFrame)
    def _end_frame(self, frame: EndFrame) -> str:
        """EndFrame handler. Terminate the call on Asterisk by sending HANGUP command when an EndFrame is processed.

        Returns properly formatted HANGUP command for Asterisk WebSocket channel when an EndFrame is processed, indicating that the call should be terminated.

        Args:
            frame: The EndFrame to be processed.
        """
        return self._asterisk_ws_proto.build("HANGUP")

    @handler(CancelFrame)
    def _cancel_frame(self, frame: CancelFrame) -> str:
        """CancelFrame handler. Terminate the call on Asterisk by sending HANGUP command when a CancelFrame is processed.

        Returns properly formatted HANGUP command for Asterisk WebSocket channel when a CancelFrame is processed, indicating that the call should be terminated.

        Args:
            frame: The CancelFrame to be processed.
        """
        return self._asterisk_ws_proto.build("HANGUP")

    @handler(InterruptionFrame)
    def _interruption_frame(self, frame: InterruptionFrame) -> str:
        """InterruptionFrame handler.

        Returns properly formatted FLUSH_MEDIA command for Asterisk WebSocket channel when an InterruptionFrame is processed,
        indicating that the buffered media on Asterisk should be flushed (bot stops speaking immediately).

        Args:
            frame: The InterruptionFrame to be processed.
        """
        return self._asterisk_ws_proto.build("FLUSH_MEDIA")

    ### Utility methods ###

    def create_resampler(self, direction: str) -> Callable[[bytes], Awaitable[bytes]]:
        """Create a resampler function to convert audio between different sample rates.

        Args:
            input_sample_rate: The sample rate of the input audio.
            output_sample_rate: The sample rate of the output audio.

        Returns:
            A function that takes raw audio bytes as input and returns resampled audio bytes.
        """

        if direction not in ["input", "output"]:
            raise ValueError(
                f"Invalid direction for resampler: {direction}, it should be either 'input' or 'output'."
            )

        if direction == "input":
            resampler_input_rate = self._asterisk_sample_rate
            resampler_output_rate = self._pipeline_in_sample_rate
        else:
            resampler_input_rate = self._pipeline_out_sample_rate
            resampler_output_rate = self._asterisk_sample_rate

        if resampler_input_rate == resampler_output_rate:
            # No resampling needed, return dummy function
            logger.warning(
                f"Dummy resampler created for [{direction}] direction, in_rate ({resampler_input_rate} Hz), out_rate ({resampler_output_rate} Hz)."
            )

            async def dummy(audio) -> bytes:
                return audio

            return dummy
        else:
            # Create the stateful instance of resampler
            resampler = create_stream_resampler()

            # Wrapper for that instance
            async def wrap_resample(audio) -> bytes:
                return await resampler.resample(
                    audio, resampler_input_rate, resampler_output_rate
                )

            return wrap_resample

    ### FrameSerializer interface implementation ###

    async def setup(self, frame: StartFrame):
        """Initialize the serializer with startup configuration.

        Defined to set the pipeline input sample rate for resampling.

        Args:
            frame: StartFrame containing initialization parameters.
        """
        self._pipeline_in_sample_rate = frame.audio_in_sample_rate
        self._pipeline_out_sample_rate = frame.audio_out_sample_rate

        if self._pipeline_in_sample_rate != self._pipeline_out_sample_rate:
            logger.warning(
                f"Pipeline input sample rate ({self._pipeline_in_sample_rate} Hz) != output sample rate ({self._pipeline_out_sample_rate} Hz). Please try to avoid resampling when possible."
            )

    async def serialize(self, frame: Frame) -> str | bytes | None:
        """Convert a frame to its serialized representation suitable for Asterisk WebSocket channel.

        Args:
            frame: The frame to serialize.

        Returns:
            Serialized frame data as string, bytes, or None if serialization fails.
        """
        frame_type = type(frame)
        sync_handler = self._sync_frame_handlers.get(frame_type, None)
        if sync_handler is not None:
            return sync_handler(frame)
        async_handler = self._async_frame_handlers.get(frame_type, None)
        if async_handler is not None:
            return await async_handler(frame)
        logger.trace(
            f"Received unhandled frame type in Asterisk WebSocket serializer: {frame_type}. Frame: {frame}"
        )
        return None

    async def deserialize(self, data: str | bytes) -> Frame | None:
        """Convert serialized data from Asterisk's websocket channel to a frame object.

        Args:
            data: Serialized frame data as string or bytes.

        Returns:
            Reconstructed Frame object, or None if deserialization fails.
        """

        # Handle audio
        if isinstance(data, bytes):
            # Check if input resampling is needed
            if self._pipeline_in_sample_rate != self._asterisk_sample_rate:
                if self._input_resampler is None:
                    logger.warning(
                        "Resampling is required but input resampler is not initialized, we can't resample the audio frame."
                    )
                    return None
                else:
                    logger.trace(
                        f"Resampling audio from Asterisk sample rate {self._asterisk_sample_rate} Hz to pipeline input sample rate {self._pipeline_in_sample_rate} Hz."
                    )
                    resampled_audio = await self._input_resampler(data)
                    if resampled_audio is None or len(resampled_audio) == 0:
                        logger.trace("Resampled audio contains no data.")
                        return None
            else:
                logger.trace("Forwarding audio from Asterisk without resampling.")
                resampled_audio = data
            return InputAudioRawFrame(
                audio=resampled_audio,
                num_channels=1,
                sample_rate=self._pipeline_in_sample_rate,
            )

        # Handle events
        elif isinstance(data, str):
            event = self._asterisk_ws_proto.parse(data)

            if event is not None:
                return await self._handle_event(event)
            else:
                logger.warning(
                    f"Failed to parse Asterisk WebSocket event from data: {data}"
                )
                return None
        else:
            logger.warning(
                f"Received data of unsupported type from Asterisk WebSocket channel: {type(data)}. Data: {data}"
            )
            return None
