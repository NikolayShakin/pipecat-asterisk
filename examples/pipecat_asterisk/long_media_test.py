#
# Copyright (c) 2026, Nikolai Shakin
#
# SPDX-License-Identifier: BSD-2-Clause
#

import asyncio
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
import sys
from loguru import logger

from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask

from pipecat_asterisk import AsteriskWebsocketTransport, FileAudioGenerator, WhiteNoiseGenerator

load_dotenv(override=True)

logger.remove(0)
logger.add(sys.stderr, level="DEBUG")

async def run_bot(websocket_client):

    ws_transport = AsteriskWebsocketTransport(websocket=websocket_client)

    # white_noise = WhiteNoiseGenerator(sampling_rate=16000)
    audio_file_generator = FileAudioGenerator(sampling_rate=16000, file_path="slin16_1k.raw")

    pipeline = Pipeline(
        [
            ws_transport.input(),
            # white_noise,
            audio_file_generator,
            ws_transport.output(),
        ]
    )

    task = PipelineTask(
        pipeline,
    )

    @ws_transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        # Kick off the conversation as soon as the WebSocket connection is established
        await task.queue_frames([LLMRunFrame()])

    @ws_transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        await task.cancel()

    runner = PipelineRunner(handle_sigint=False)

    await runner.run(task)


app = FastAPI()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        await run_bot(websocket)
    except Exception as e:
        print(f"Exception in run_bot: {e}")


async def main():
    config = uvicorn.Config(app, host="0.0.0.0", port=7860)
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Application stopped gracefully.")
