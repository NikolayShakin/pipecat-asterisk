#
# Copyright (c) 2026, Nikolai Shakin
#
# SPDX-License-Identifier: BSD-2-Clause
#

import asyncio
import os
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
import sys
from loguru import logger

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService

from pipecat_asterisk import AsteriskWebsocketTransport

load_dotenv(override=True)

logger.remove(0)
logger.add(sys.stderr, level="DEBUG")


SYSTEM_INSTRUCTION = """
You are voice Gemini Chatbot, and a poet Martin Silenus.
Be creative, respond in a poetic manner when appropriate.
Be concise. Not more than 3 sentences.
Demonstrate what you can do. 
"""


async def run_bot(websocket_client):

    ws_transport = AsteriskWebsocketTransport(websocket=websocket_client)

    if api_key := os.getenv("GOOGLE_API_KEY"):
        llm = GeminiLiveLLMService(
            api_key=api_key,
            settings=GeminiLiveLLMService.Settings(
                voice="Puck",
                system_instruction=SYSTEM_INSTRUCTION,
            ),
        )
    else:
        logger.error(
            "GOOGLE_API_KEY environment variable not set. Please set it to run the bot."
        )
        return

    context = LLMContext(
        [
            {
                "role": "user",
                "content": "Start by greeting the user warmly and introducing yourself.",
            }
        ],
    )
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=SileroVADAnalyzer(),
        ),
    )

    pipeline = Pipeline(
        [
            ws_transport.input(),
            user_aggregator,
            llm,
            ws_transport.output(),
            assistant_aggregator,
        ]
    )

    task = PipelineTask(
        pipeline,
    )

    @ws_transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info("Pipecat client connected.")
        # Kick off the conversation as soon as the WebSocket connection is established
        await task.queue_frames([LLMRunFrame()])

    @ws_transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info("Pipecat Client disconnected")
        await task.cancel()

    runner = PipelineRunner(handle_sigint=False)

    await runner.run(task)


app = FastAPI()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("WebSocket connection accepted")
    try:
        await run_bot(websocket)
    except Exception as e:
        print(f"Exception in run_bot: {e}")


async def main():
    config = uvicorn.Config(app, host="127.0.0.1", port=7860)
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Application stopped gracefully.")
