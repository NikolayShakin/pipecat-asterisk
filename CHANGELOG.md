# Changelog

## [Unreleased]
## [0.1.0] - 2026-05-04
### Added
- Initial release

## [0.1.1] - 2026-05-21
### Added
- Add long_audio_test
- Utilities for simulating long audio stream (FileAudioGenerator, WhiteNoiseGenerator)
### Fixed
- Fix bug remote buffer overflow when long audio is being processed

## [0.1.2] - 2026-06-10
### Added
- Handling for `AsteriskCommandFrame` in the transport, which allows sending arbitrary commands to the Asterisk websocket channel.
- Project structure improved, frames moved to a separate module `frames.py` thanks to the contribution of @abalashov.
