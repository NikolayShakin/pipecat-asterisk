"""A stand-in for the WebSocket Asterisk opens to the application, for transport tests.

`FakeAsteriskWebSocket` is what the transport reads from and writes to: a test puts events on
it the way Asterisk would send them, and reads back everything the transport sent, with the
time it was sent. `running_pipeline` runs the transport's input and output in a pipeline.
"""

import asyncio
import json
import time
from contextlib import asynccontextmanager

from starlette.websockets import WebSocketState

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.workers.runner import WorkerRunner

SAMPLE_RATE = 16000
FRAME_BYTES = 640  # 20 ms of slin16

MEDIA_START = (
    "MEDIA_START connection_id:test channel:WebSocket/test-00000001 channel_id:1700000000.1 "
    f"format:slin16 optimal_frame_size:{FRAME_BYTES} ptime:20"
)
MEDIA_START_JSON = json.dumps(
    {
        "event": "MEDIA_START",
        "connection_id": "test",
        "channel": "WebSocket/test-00000001",
        "channel_id": "1700000000.1",
        "format": "slin16",
        "optimal_frame_size": FRAME_BYTES,
        "ptime": 20,
        "channel_variables": {},
    }
)


class FakeAsteriskWebSocket:
    """The WebSocket as the transport sees it, with Asterisk at the other end."""

    def __init__(self):
        self.client_state = WebSocketState.CONNECTED
        self.application_state = WebSocketState.CONNECTED
        self.headers = {}
        self.sent: list[tuple[float, str | bytes]] = []
        self.closed = asyncio.Event()
        self._incoming: asyncio.Queue = asyncio.Queue()

    def send_event(self, text: str):
        """Asterisk sends a TEXT message."""
        self._incoming.put_nowait({"type": "websocket.receive", "text": text})

    def send_audio(self, data: bytes):
        """Asterisk sends a BINARY message: the caller's audio."""
        self._incoming.put_nowait({"type": "websocket.receive", "bytes": data})

    async def receive(self):
        return await self._incoming.get()

    async def send_text(self, data: str):
        self.sent.append((time.monotonic(), data))

    async def send_bytes(self, data: bytes):
        self.sent.append((time.monotonic(), data))

    async def close(self, code: int = 1000, reason: str | None = None):
        self.client_state = WebSocketState.DISCONNECTED
        self.application_state = WebSocketState.DISCONNECTED
        self.closed.set()
        self._incoming.put_nowait({"type": "websocket.disconnect", "code": code})

    @property
    def commands(self) -> list[str]:
        """The commands (TEXT messages) the transport sent, in order, in plain text."""
        return [json.loads(data)["command"] if data.startswith("{") else data for _, data in self.sent if isinstance(data, str)]

    @property
    def audio(self) -> bytes:
        """All the audio the transport sent, joined."""
        return b"".join(data for _, data in self.sent if isinstance(data, bytes))

    def time_of(self, command: str) -> float:
        """When the transport sent `command`."""
        return next(at for at, data in self.sent if isinstance(data, str) and command in data)


@asynccontextmanager
async def running_pipeline(transport, *processors):
    """Run `transport.input()`, `processors`, `transport.output()` until the block exits."""
    worker = PipelineWorker(
        Pipeline([transport.input(), *processors, transport.output()]),
        params=PipelineParams(audio_in_sample_rate=SAMPLE_RATE, audio_out_sample_rate=SAMPLE_RATE),
    )
    started = asyncio.Event()

    @worker.event_handler("on_pipeline_started")
    async def on_pipeline_started(worker, frame):
        started.set()

    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    running = asyncio.create_task(runner.run())
    await asyncio.wait_for(started.wait(), 5)
    try:
        yield worker
    finally:
        if not running.done():
            await worker.cancel()
        await asyncio.wait_for(running, 10)
