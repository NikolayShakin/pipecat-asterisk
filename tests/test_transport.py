"""Tests for AsteriskWebsocketOutputTransport frame processing and flow control."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from pipecat.frames.frames import (
    BotStoppedSpeakingFrame,
    TTSStoppedFrame,
)
from pipecat.processors.frame_processor import FrameDirection

from pipecat_asterisk.transport.transport import AsteriskWebsocketOutputTransport


def _make_output() -> AsteriskWebsocketOutputTransport:
    transport = MagicMock()
    transport.cleanup = AsyncMock()
    client = MagicMock()
    client.send = AsyncMock()
    client.disconnect = AsyncMock()
    return AsteriskWebsocketOutputTransport(transport, client)


@pytest.mark.asyncio
async def test_push_frame_holds_bot_stopped_speaking_frame_until_fence_set(monkeypatch):
    """BotStoppedSpeakingFrame must wait until bot_stopped_speaking_fence is set."""
    output = _make_output()
    output._flow_controller = MagicMock()
    fence = asyncio.Event()
    output._flow_controller.bot_stopped_speaking_fence = fence

    pushed_frames = []

    async def mock_super_push(self, frame, direction=FrameDirection.DOWNSTREAM):
        pushed_frames.append((frame, direction))

    monkeypatch.setattr(AsteriskWebsocketOutputTransport.mro()[1], "push_frame", mock_super_push)

    frame = BotStoppedSpeakingFrame()
    push_task = asyncio.create_task(output.push_frame(frame, FrameDirection.DOWNSTREAM))

    await asyncio.sleep(0.01)
    assert not push_task.done()
    assert len(pushed_frames) == 0

    fence.set()
    await push_task

    assert len(pushed_frames) == 1
    assert pushed_frames[0] == (frame, FrameDirection.DOWNSTREAM)


@pytest.mark.asyncio
async def test_push_frame_does_not_hold_bot_stopped_speaking_if_fence_already_set(monkeypatch):
    """If fence is already set, BotStoppedSpeakingFrame is pushed immediately."""
    output = _make_output()
    output._flow_controller = MagicMock()
    fence = asyncio.Event()
    fence.set()
    output._flow_controller.bot_stopped_speaking_fence = fence

    pushed_frames = []

    async def mock_super_push(self, frame, direction=FrameDirection.DOWNSTREAM):
        pushed_frames.append((frame, direction))

    monkeypatch.setattr(AsteriskWebsocketOutputTransport.mro()[1], "push_frame", mock_super_push)

    frame = BotStoppedSpeakingFrame()
    await output.push_frame(frame, FrameDirection.DOWNSTREAM)

    assert len(pushed_frames) == 1
    assert pushed_frames[0] == (frame, FrameDirection.DOWNSTREAM)


@pytest.mark.asyncio
async def test_push_frame_does_not_hold_tts_stopped_frame(monkeypatch):
    """TTSStoppedFrame should pass through push_frame without waiting on the fence."""
    output = _make_output()
    output._flow_controller = MagicMock()
    fence = asyncio.Event()  # fence is NOT set
    output._flow_controller.bot_stopped_speaking_fence = fence

    pushed_frames = []

    async def mock_super_push(self, frame, direction=FrameDirection.DOWNSTREAM):
        pushed_frames.append((frame, direction))

    monkeypatch.setattr(AsteriskWebsocketOutputTransport.mro()[1], "push_frame", mock_super_push)

    frame = TTSStoppedFrame()
    push_task = asyncio.create_task(output.push_frame(frame, FrameDirection.DOWNSTREAM))

    await asyncio.sleep(0.01)
    assert push_task.done()
    assert len(pushed_frames) == 1
    assert pushed_frames[0] == (frame, FrameDirection.DOWNSTREAM)
