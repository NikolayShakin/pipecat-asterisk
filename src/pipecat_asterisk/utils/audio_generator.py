import asyncio
import os
from pipecat.frames.frames import (
    InputTransportMessageFrame,
    OutputAudioRawFrame,
    CancelFrame, 
    TTSStoppedFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
)
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection

class AudioGenerator(FrameProcessor):
        def __init__(self, sampling_rate: int = 8000):
            super().__init__()
            self._task = None
            self._sampling_rate = sampling_rate

        async def _generate_audio(self):
            pass
                
        async def process_frame(self, frame, direction=FrameDirection.DOWNSTREAM):
            await super().process_frame(frame, direction)
            if (
                isinstance(frame, InputTransportMessageFrame) 
                and frame.message.get("event", None) == "MEDIA_START"
                ):
                self._task = asyncio.create_task(self._generate_audio())
            elif isinstance(frame, CancelFrame):
                if self._task:
                    self._task.cancel()
            await self.push_frame(frame)

class FileAudioGenerator(AudioGenerator):
    def __init__(self, file_path: str, sampling_rate: int = 8000):
        super().__init__(sampling_rate=sampling_rate)
        self._file_path = file_path

    async def _generate_audio(self):
        try:
            # Load audio from file to memory
            audio_data = None
            try:
                with open(self._file_path, "rb") as f:
                    audio_data = f.read()
                    batch_size = self._sampling_rate * 2 # 1 second of audio at the given sampling rate (e.g. slin = 2 bytes per sample)
                    while True:
                        await self.push_frame(TTSStartedFrame())
                        for i in range(0, len(audio_data), batch_size):
                            chunk = audio_data[i:i+batch_size]
                            frame = TTSAudioRawFrame(
                                audio=chunk,
                                sample_rate=self._sampling_rate,
                                num_channels=1,
                            )
                            await self.push_frame(frame)
                            await asyncio.sleep(0.2)

                        await self.push_frame(TTSStoppedFrame())
                        await asyncio.sleep(5)
            except FileNotFoundError:
                print(f"File not found: {self._file_path} in the current working directory: {os.getcwd()}")
        except asyncio.CancelledError:
            pass

class WhiteNoiseGenerator(AudioGenerator):
        def __init__(self, sampling_rate: int = 8000):
            super().__init__(sampling_rate=sampling_rate)

        async def _generate_audio(self):
            batch_size = self._sampling_rate * 2 # 1 second of audio at the given sampling rate (e.g. slin16 = 2 bytes per sample)
            try:
                while True:
                    audio_data = os.urandom(int(batch_size))
                    frame = OutputAudioRawFrame(audio=audio_data, sample_rate=self._sampling_rate, num_channels=1)
                    await self.push_frame(frame)
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                pass
