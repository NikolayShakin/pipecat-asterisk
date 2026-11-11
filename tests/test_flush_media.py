"""Tests for what the transport sends Asterisk on an interruption."""

import asyncio

import pytest

from pipecat.frames.frames import InterruptionFrame

from pipecat_asterisk import AsteriskWebsocketTransport

from .fake_asterisk import MEDIA_START, MEDIA_START_JSON, FakeAsteriskWebSocket, running_pipeline


@pytest.mark.parametrize("media_start", [MEDIA_START, MEDIA_START_JSON], ids=["plain-text", "json"])
async def test_interruption_flushes_and_restarts_media_buffering(media_start):
    """FLUSH_MEDIA ends media buffering on Asterisk, so START_MEDIA_BUFFERING must follow it."""
    websocket = FakeAsteriskWebSocket()
    transport = AsteriskWebsocketTransport(websocket)
    async with running_pipeline(transport) as worker:
        websocket.send_event(media_start)
        await asyncio.sleep(0.1)
        await worker.queue_frame(InterruptionFrame())
        await asyncio.sleep(0.1)
        assert websocket.commands == ["START_MEDIA_BUFFERING", "FLUSH_MEDIA", "START_MEDIA_BUFFERING"]


async def test_interruption_before_media_start_sends_no_buffering_command():
    websocket = FakeAsteriskWebSocket()
    transport = AsteriskWebsocketTransport(websocket)
    async with running_pipeline(transport) as worker:
        await worker.queue_frame(InterruptionFrame())
        await asyncio.sleep(0.1)
        assert "START_MEDIA_BUFFERING" not in websocket.commands
