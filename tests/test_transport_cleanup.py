"""Tests for what the output transport leaves running when the pipeline ends."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from pipecat.frames.frames import InputTransportMessageFrame

from pipecat_asterisk.transport.transport import AsteriskWebsocketOutputTransport

MEDIA_START = {"event": "MEDIA_START", "optimal_frame_size": "640", "ptime": "20"}


def _make_output() -> AsteriskWebsocketOutputTransport:
    transport = MagicMock()
    transport.cleanup = AsyncMock()
    client = MagicMock()
    client.send = AsyncMock()
    client.disconnect = AsyncMock()
    return AsteriskWebsocketOutputTransport(transport, client)


@pytest.mark.asyncio
async def test_cleanup_stops_the_flow_control_task():
    """The task started at MEDIA_START must not outlive the pipeline."""
    output = _make_output()
    await output._media_start_handler(InputTransportMessageFrame(message=MEDIA_START))
    task = output._flow_controller._flow_control
    assert not task.done()

    await output.cleanup()
    await asyncio.sleep(0)  # the cancellation is delivered on the loop's next turn

    assert task.cancelled()
    assert output._flow_controller is None


@pytest.mark.asyncio
async def test_cleanup_before_media_start_does_nothing():
    """A call that ends before MEDIA_START has no flow controller to stop."""
    output = _make_output()

    await output.cleanup()

    assert output._flow_controller is None
