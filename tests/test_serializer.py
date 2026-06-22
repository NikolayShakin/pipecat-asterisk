"""Unit tests for AsteriskFrameSerializer.

"""

import pytest

from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    InterruptionFrame,
    InputDTMFFrame,
    InputTransportMessageFrame,
    StartFrame,
)

from pipecat_asterisk.serializer.serializer import AsteriskFrameSerializer
from pipecat_asterisk.frames.frames import AsteriskCommandFrame

@pytest.mark.parametrize(
    "frame, expected_cmd",
    [
        (EndFrame(), "HANGUP"),
        (CancelFrame(), "HANGUP"),
        (InterruptionFrame(), "FLUSH_MEDIA"),
        (AsteriskCommandFrame("START_MEDIA_BUFFERING"), "START_MEDIA_BUFFERING"),
        (AsteriskCommandFrame("REPORT_QUEUE_DRAINED"), "REPORT_QUEUE_DRAINED"),
    ],
)
async def test_serialize(frame, expected_cmd):
    """Every Pipecat Frame must produce a respective Asterisk command via serialize().

    """
    serializer = AsteriskFrameSerializer()
    result = await serializer.serialize(frame)
    assert isinstance(result, str)
    assert expected_cmd in result

async def test_serialize_unhandled_frame():
    """Frames without a registered handler fall through to None.

    `StartFrame` is currently unhandled.
    """
    serializer = AsteriskFrameSerializer()
    result = await serializer.serialize(
        StartFrame(audio_in_sample_rate=16000, audio_out_sample_rate=16000)
    )
    assert result is None

async def test_serialize_audio_no_resampling():
    """OutputAudioRawFrame goes through the async handler.

    """
    from pipecat.frames.frames import OutputAudioRawFrame

    serializer = AsteriskFrameSerializer()
    serializer._pipeline_out_sample_rate = 16000
    serializer._asterisk_sample_rate = 16000

    audio = b"\x00\x01" * 3200
    frame = OutputAudioRawFrame(audio=audio, sample_rate=16000, num_channels=1)
    result = await serializer.serialize(frame)
    assert result == audio

# TODO: Add tests for resampled audio, especially verify that start and end of the audio are properly resampled
# and don't have artifacts of missing data.

@pytest.mark.parametrize(
    "event",
    [
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "1"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "2"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "3"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "4"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "5"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "6"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "7"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "8"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "9"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "0"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "#"}),
        ({"event": "DTMF_END", "channel_id": "123456789", "digit": "*"}),
    ],
)
async def test_handle_event_dispatches_dtmf_end(event):
    from pipecat.audio.dtmf.types import KeypadEntry
    serializer = AsteriskFrameSerializer()
    frame = await serializer._handle_event(event)
    assert isinstance(frame, InputDTMFFrame)
    assert frame.button == KeypadEntry(event["digit"])

@pytest.mark.parametrize(
    "event, frame_type",
    [
        ({"event": "MEDIA_XOFF", "channel_id": "123456789"}, type(None)),
        ({"event": "MEDIA_XON", "channel_id": "123456789"}, type(None)),
        ({"event": "QUEUE_DRAINED", "channel_id": "123456789"}, InputTransportMessageFrame),
        (   {
                "event": "MEDIA_START",
                "connection_id": "e226e283-c90a-4ea9-9e37-389000b9ef47",
                "channel": "WebSocket/connectionid",
                "channel_id": "pbx1-123456789.999",
                "format": "slin16",
                "optimal_frame_size": 160,
                "ptime": 20,
                "channel_variables": {
                    "SOME_CHANNEL_VARIABLE": "some value",
                    "ANOTHER_CHANNEL_VARIABLE": "some other value"
                }
            }, InputTransportMessageFrame
        ),
    ],
)
async def test_handle_event(event, frame_type):
    serializer = AsteriskFrameSerializer()
    frame = await serializer._handle_event(event)
    assert isinstance(frame, frame_type)


async def test_handle_event_dispatches_xoff_and_xon_to_none():
    """MEDIA_XOFF and MEDIA_XON handlers log but return None.

    The handlers are registered, so the dispatch must find them and not
    fall through to the "unhandled" branch.
    """
    serializer = AsteriskFrameSerializer()
    assert await serializer._handle_event({"event": "MEDIA_XOFF"}) is None
    assert await serializer._handle_event({"event": "MEDIA_XON"}) is None

async def test_handle_event_missing_event_field_returns_none():
    serializer = AsteriskFrameSerializer()
    assert await serializer._handle_event({"not_event": "foo"}) is None

async def test_handle_event_unknown_event_returns_none():
    serializer = AsteriskFrameSerializer()
    assert await serializer._handle_event({"event": "SOMETHING_NEW"}) is None

def test_frame_handler_tables_have_no_overlap():
    """A frame class registered in both sync and async tables would
    produce ambiguous dispatch (sync wins by lookup order). Catch that
    here so the registration stays unambiguous.
    """
    serializer = AsteriskFrameSerializer()
    overlap = set(serializer._sync_frame_handlers) & set(
        serializer._async_frame_handlers
    )
    assert overlap == set()
