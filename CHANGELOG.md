# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Added
- 

### Changed
- 

### Fixed
- 

---

## [0.1.7] - 2026-02-04

### Added
- Added all project related URL's in pyproject.toml
- Added Storage Fetch Feature to pick up files directly from any storage.
    - Added support for medallion and standard storage
    - Added support for both multi/single file.
    - Currently supported formats; `*.xml`, `*.json`, `*.parquet`
    - Implemented schema enforcement.

### Changed
- Removed unused exception classes and made the error statements more readable.

### Fixed
- Fixed some exception classes which were not showing error messages correctly.

---

## [0.1.6] - 2026-02-04

### Changed
- 

### Fixed
- Fixed pypi unverified issue

---

## [0.1.5] - 2026-02-04

### Changed
- Corrected release workflow to properly generate GitHub release notes.
- Improved version validation to prevent duplicate releases.

### Fixed
- GitHub releases incorrectly using merge commit messages as release notes.

---

## [0.1.4] - 2026-02-04

### Added
- Validation for primary key consistency in SCD Type-2 tables.
- Configurable retry logic for failed Delta Lake writes.
- Optional strict mode for schema evolution checks.

### Changed
- Improved performance of incremental loads by optimizing merge conditions.
- Refactored internal metadata handling for better extensibility.

### Fixed
- Incorrect handling of nullable fields during schema comparison.
- Edge case where late-arriving records caused duplicate version rows.
- Logging issue where validation warnings were silently ignored.

---

## [0.1.3] - 2026-01-31

### Added
- Support for custom partitioning strategies in Delta tables.
- New CLI flag to validate schemas without executing writes.

### Fixed
- Bug causing SCD end dates to be incorrectly updated on no-op changes.

---

## [0.1.2] - 2026-01-29

### Changed
- Updated internal API for schema enforcement to improve clarity.
- Improved error messages for invalid configuration files.

---

## [0.1.1] - 2026-01-28

### Fixed
- Packaging issue that excluded required metadata files from PyPI distribution.

---

## [0.1.0] - 2026-01-28

### Added
- Initial release of SDMF.
- Schema enforcement for structured data ingestion.
- Support for Slowly Changing Dimensions (SCD Type-2).
- Delta Lake integration for reliable, versioned data storage.
- Core project scaffolding and configuration.
