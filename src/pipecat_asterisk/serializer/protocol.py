#
# Copyright (c) 2026, Nikolai Shakin
#
# SPDX-License-Identifier: BSD-2-Clause
#

from loguru import logger
import json


class AsteriskWSProtocol:
    """Asterisk WebSocket protocol handler.

    The protocol learns Asterisk web_socket channel subprotocol (json or plain-text) based on the first event "MEDIA_START", if it is not set explicitly.
    It parses events accordingly to the identified subprotocol and returns dictionary objects.
    It builds commands to Asterisk in the identified subprotocol.
    """

    def __init__(self, subprotocol: str | None = None):
        self._sub_protocol = subprotocol
        if self._sub_protocol is not None and self._sub_protocol not in [
            "json",
            "plain-text",
        ]:
            raise ValueError(
                f"Invalid subprotocol for AsteriskWSProtocol: {self._sub_protocol}, it should be either 'json' or 'plain-text' or None for autodetect."
            )
        if self._sub_protocol is None:
            logger.debug(
                "Asterisk subprotocol ['json' or 'plain-text'] is not defined, we will try to learn it automatically based on 'MEDIA_START' event."
            )

    def parse(self, event: str) -> dict | None:
        """Parses the event from Asterisk WebSocket channel and return a dictionary object."""
        if self._sub_protocol is None:
            # If subprotocol is not set, we attempt to identify the format based on the first event we receive, which should be "MEDIA_START"
            if "MEDIA_START" in event:
                try:
                    json.loads(event)
                    self._sub_protocol = "json"
                    logger.debug(
                        "Identified Asterisk subprotocol as JSON based on MEDIA_START event format."
                    )
                except json.JSONDecodeError:
                    self._sub_protocol = "plain-text"
                    logger.debug(
                        "Identified Asterisk subprotocol as plain-text based on MEDIA_START event format. Notice: In plain-text format you will not be able to read Asterisk channel variables."
                    )
            else:
                logger.warning(
                    f'We tried to auto-detect the Asterisk subprotocol but, received the event before "MEDIA_START". Event: {event}'
                )
                return None

        if self._sub_protocol == "json":
            try:
                return json.loads(event)
            except json.JSONDecodeError as e:
                logger.error(
                    f"Failed to parse Asterisk WebSocket event as JSON: {event}"
                )
                raise e
        else:
            event_entries = event.split(" ")
            if event_entries[0] == "":
                logger.warning(f"Received empty event from Asterisk WebSocket channel.")
                return None
            event_dict = {"event": event_entries.pop(0)}
            for entry in event_entries:
                if ":" in entry:
                    key, value = entry.split(":", 1)
                    event_dict[key] = value
            return event_dict

    def build(self, command: str) -> str:
        """Returns a properly formatted command for Asterisk WebSocket channel based on the identified subprotocol."""
        if self._sub_protocol == "plain-text":
            return command
        else:
            return f'{{"command": "{command}"}}'
