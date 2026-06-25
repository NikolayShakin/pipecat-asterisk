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

## [0.1.3] - 2026-06-24
### Added
- Added unit tests, thanks to the contribution of @Salman778
### Changed
- Improved performance, thanks to the contribution of @Salman778
- Improved API, added decorator `@handler` for registering event and frame handlers in the serializer
- Handlers can be sync/async, and they are automatically registered to a respective dictionary
- Flow controller's `close` function is now async
- Improved documentation
- Updated examples