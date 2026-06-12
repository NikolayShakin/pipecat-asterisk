"""Unit tests for FlowController.

Covers the deque-buffer drain semantics, drop_buffer state reset,
and that the async close(gracefully=True) yields the event loop so the
flow_control task can actually drain.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from pipecat_asterisk.transport.flow_controller import FlowController


PTIME_MS = 20
PSIZE_BYTES = 640  # slin16 @ 20ms == 16000 Hz * 2 bytes/sample * 0.02 s


def _make_controller() -> FlowController:
    """Build a controller with a mock websocket client.

    The controller starts its `flow_control` task in `__init__`, so this
    must be called from inside a running event loop.
    """
    client = MagicMock()
    client.send = AsyncMock()
    return FlowController(ptime=PTIME_MS, psize=PSIZE_BYTES, websocket_client=client)


# ---------------------------------------------------------------------------
# __call__ / buffer accounting
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_call_appends_chunk_and_tracks_size():
    fc = _make_controller()
    try:
        fc(b"\x00" * 640)
        fc(b"\x01" * 320)
        assert fc._local_buffer_size == 960
        assert list(fc._local_buffer) == [b"\x00" * 640, b"\x01" * 320]
    finally:
        await fc.close()


@pytest.mark.asyncio
async def test_call_ignores_empty_chunk():
    fc = _make_controller()
    try:
        fc(b"")
        assert fc._local_buffer_size == 0
        assert len(fc._local_buffer) == 0
    finally:
        await fc.close()


# ---------------------------------------------------------------------------
# _pop_bytes drain semantics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pop_bytes_drains_whole_chunks():
    """Popping a budget that lines up on chunk boundaries pops chunks whole."""
    fc = _make_controller()
    try:
        fc(b"a" * 100)
        fc(b"b" * 100)
        fc(b"c" * 100)
        out = fc._pop_bytes(200)
        assert out == b"a" * 100 + b"b" * 100
        assert fc._local_buffer_size == 100
        assert list(fc._local_buffer) == [b"c" * 100]
    finally:
        await fc.close()


@pytest.mark.asyncio
async def test_pop_bytes_slices_head_chunk():
    """A budget smaller than the head chunk slices the head and leaves the tail."""
    fc = _make_controller()
    try:
        fc(b"abcdef")
        out = fc._pop_bytes(3)
        assert out == b"abc"
        assert fc._local_buffer_size == 3
        assert list(fc._local_buffer) == [b"def"]
    finally:
        await fc.close()


@pytest.mark.asyncio
async def test_pop_bytes_handles_mix_of_whole_and_partial():
    """A budget that straddles a chunk boundary pops one whole chunk + slices the next."""
    fc = _make_controller()
    try:
        fc(b"a" * 100)
        fc(b"b" * 100)
        out = fc._pop_bytes(150)
        assert out == b"a" * 100 + b"b" * 50
        assert fc._local_buffer_size == 50
        assert list(fc._local_buffer) == [b"b" * 50]
    finally:
        await fc.close()


@pytest.mark.asyncio
async def test_pop_bytes_caps_at_buffer_size():
    """Asking for more bytes than are buffered returns everything available."""
    fc = _make_controller()
    try:
        fc(b"hello")
        out = fc._pop_bytes(1_000_000)
        assert out == b"hello"
        assert fc._local_buffer_size == 0
        assert len(fc._local_buffer) == 0
    finally:
        await fc.close()


@pytest.mark.asyncio
async def test_pop_bytes_returns_empty_when_empty():
    fc = _make_controller()
    try:
        assert fc._pop_bytes(100) == b""
        assert fc._pop_bytes(0) == b""
    finally:
        await fc.close()


# ---------------------------------------------------------------------------
# drop_buffer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drop_buffer_resets_deque_size_and_utilization():
    fc = _make_controller()
    try:
        fc(b"a" * 100)
        fc(b"b" * 100)
        fc._remote_buffer_utilization = 1234.5
        fc.drop_buffer()
        assert fc._local_buffer_size == 0
        assert len(fc._local_buffer) == 0
        assert fc._remote_buffer_utilization == 0.0
    finally:
        await fc.close()


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_non_graceful_cancels_immediately():
    fc = _make_controller()
    task = fc._flow_control
    fc(b"x" * 1024)
    await fc.close(gracefully=False)
    with pytest.raises(asyncio.CancelledError):
        await task
    # Non-graceful close leaves the unsent buffer in place by design.
    assert fc._local_buffer_size == 1024


@pytest.mark.asyncio
async def test_close_graceful_yields_loop_and_drains():
    """The bug fix: close(gracefully=True) must `await asyncio.sleep`, not
    `time.sleep`, so the flow_control task can actually run and drain the
    buffer. We prove this by checking that close returns once
    _local_buffer_size hits zero - which can only happen if flow_control
    was given CPU during the wait.
    """
    fc = _make_controller()
    try:
        # Buffer one chunk worth of audio. Low water mark logic in
        # flow_control will dispatch it on the next tick.
        fc(b"\x00" * PSIZE_BYTES)
        assert fc._local_buffer_size == PSIZE_BYTES

        # If close used time.sleep, this would hang the event loop until
        # the cancellation, and flow_control would never get a chance to
        # send. Cap the wait so a regression times out instead of hanging.
        await asyncio.wait_for(fc.close(gracefully=True), timeout=1.0)

        assert fc._local_buffer_size == 0
        fc._websocket_client.send.assert_awaited()
    finally:
        # Already cancelled above; second close() is a no-op safety net.
        await fc.close()
