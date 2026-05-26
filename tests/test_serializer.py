"""Unit tests for AsteriskFrameSerializer dispatch tables.

These tests lock in the behavior of the dict-based frame/event dispatch
introduced to replace the previous `getattr(...)` reflection. The goal is
to make sure every handler registered in `__init__` is reachable through
the public `serialize` / `_handle_event` entry points, and that unknown
inputs fall through to the same "unhandled" branches as before.
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

from pipecat_asterisk.serializer.serializer import (
    AsteriskCommandFrame,
    AsteriskFrameSerializer,
)


# ---------------------------------------------------------------------------
# serialize() — sync handlers
# ---------------------------------------------------------------------------


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
async def test_serialize_dispatches_sync_handlers(frame, expected_cmd):
    """Every entry in `_sync_frame_handlers` must be reachable via serialize().

    We don't pin the exact wire format here (that's the protocol layer's
    contract); we only check that the produced string contains the command
    word, so this stays robust if the protocol formatting changes.
    """
    serializer = AsteriskFrameSerializer()
    result = await serializer.serialize(frame)
    assert isinstance(result, str)
    assert expected_cmd in result


async def test_serialize_returns_none_for_unhandled_frame():
    """Frames not registered in either dispatch table fall through to None.

    `StartFrame` is intentionally not in `_sync_frame_handlers` or
    `_async_frame_handlers` — `setup()` handles it separately.
    """
    serializer = AsteriskFrameSerializer()
    result = await serializer.serialize(
        StartFrame(audio_in_sample_rate=16000, audio_out_sample_rate=16000)
    )
    assert result is None


# ---------------------------------------------------------------------------
# serialize() — async handler (OutputAudioRawFrame)
# ---------------------------------------------------------------------------


async def test_serialize_dispatches_async_audio_handler():
    """OutputAudioRawFrame goes through the async handler table.

    The handler short-circuits and returns the raw bytes unchanged when
    the pipeline rate matches the Asterisk rate, so this also exercises
    the no-resampling fast path.
    """
    from pipecat.frames.frames import OutputAudioRawFrame

    serializer = AsteriskFrameSerializer()
    # Match rates so the handler returns audio without going through a
    # resampler that isn't initialized in this test.
    serializer._pipeline_out_sample_rate = 16000
    serializer._asterisk_sample_rate = 16000

    audio = b"\x00\x01" * 320  # 640 bytes — one slin16 frame
    frame = OutputAudioRawFrame(audio=audio, sample_rate=16000, num_channels=1)
    result = await serializer.serialize(frame)
    assert result == audio


# ---------------------------------------------------------------------------
# serialize() — sync handlers must NOT be awaited
# ---------------------------------------------------------------------------


async def test_serialize_sync_handler_result_is_not_a_coroutine():
    """Regression guard for the sync/async split.

    The previous implementation called `inspect.isawaitable` on every
    return value. After this refactor sync handlers are looked up in a
    separate table and must return a plain string, never a coroutine.
    """
    import inspect as _inspect

    serializer = AsteriskFrameSerializer()
    result = await serializer.serialize(EndFrame())
    assert not _inspect.isawaitable(result)
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# _handle_event() — registered events
# ---------------------------------------------------------------------------


def test_handle_event_dispatches_dtmf_end():
    serializer = AsteriskFrameSerializer()
    frame = serializer._handle_event({"event": "DTMF_END", "digit": "5"})
    assert isinstance(frame, InputDTMFFrame)


def test_handle_event_dispatches_queue_drained():
    serializer = AsteriskFrameSerializer()
    msg = {"event": "QUEUE_DRAINED"}
    frame = serializer._handle_event(msg)
    assert isinstance(frame, InputTransportMessageFrame)
    assert frame.message == msg


def test_handle_event_dispatches_xoff_and_xon_to_none():
    """MEDIA_XOFF and MEDIA_XON handlers log but return None.

    The handlers are registered, so the dispatch must find them and not
    fall through to the "unhandled" branch.
    """
    serializer = AsteriskFrameSerializer()
    assert serializer._handle_event({"event": "MEDIA_XOFF"}) is None
    assert serializer._handle_event({"event": "MEDIA_XON"}) is None


# ---------------------------------------------------------------------------
# _handle_event() — missing / unknown event
# ---------------------------------------------------------------------------


def test_handle_event_missing_event_field_returns_none():
    serializer = AsteriskFrameSerializer()
    assert serializer._handle_event({"not_event": "foo"}) is None


def test_handle_event_unknown_event_returns_none():
    serializer = AsteriskFrameSerializer()
    assert serializer._handle_event({"event": "SOMETHING_NEW"}) is None


# ---------------------------------------------------------------------------
# Dispatch table integrity
# ---------------------------------------------------------------------------


def test_event_handler_table_keys_are_uppercase_event_names():
    """The dispatch keys must match Asterisk's wire-format event names exactly.

    The chan_websocket protocol uses uppercase event names like
    `MEDIA_START`, `DTMF_END`, `QUEUE_DRAINED`. A lowercase key would
    silently fall through the dispatch.
    """
    serializer = AsteriskFrameSerializer()
    assert all(k == k.upper() for k in serializer._event_handlers)
    assert "MEDIA_START" in serializer._event_handlers
    assert "DTMF_END" in serializer._event_handlers
    assert "QUEUE_DRAINED" in serializer._event_handlers


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
