# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.1] 2026-08-29
### Added
- Added `:user-credentials` option to `jet-connect` for NSC/decentralized JWT auth via a
  NATS credentials file (wraps `nats.UserCredentials`).
- Add :client-cert connect option
- Add :ignore-discovered-servers connect option
- Add :nkey-option-from-seed connect option
- Add :permission-err-on-subscribe connect option
- Add :reconnect-on-flusher-error connect option
- Add :root-cas connect option
- Add :skip-subject-validation connect option
- Add :user-credential-bytes connect option
- Add :user-jwt-and-seed connect option
- Update user-credentials to allow a list of files.

### Changed
- Updated Slip version to v1.5.1

## [1.5.0] - 2026-06-26
### Changed
- Updated Slip version to v1.5.0

## [1.4.1] - 2026-05-14
### Changed
- Updated to v1.4.1 of slip.

## [1.2.0] - 2025-09-06
### Added
- Added methods to support push-consumers.
### Changed
- Updated to v1.2.0 of slip.

## [1.1.0] - 2025-06-23
### Changed
- Updated to v1.1.0 of slip.

## [1.0.2] - 2025-04-28
### Changed
- Updated to v1.0.2 of slip.

## [1.0.1]] - 2025-04-27
### Changed
- Updated to v1.0.1 of slip.
