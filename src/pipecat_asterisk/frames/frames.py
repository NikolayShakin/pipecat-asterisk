#
# Copyright (c) 2026, Nikolai Shakin
#
# SPDX-License-Identifier: BSD-2-Clause
#

from dataclasses import dataclass

from pipecat.frames.frames import Frame

@dataclass
class AsteriskCommandFrame(Frame):
    """A frame representing a command to be sent to Asterisk WebSocket channel."""
    cmd: str

