# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.5.4] - 2023-10-19

### Changed

- HDX provider code and name change

## [0.5.3] - 2023-10-19

### Fixed

- DB export GitHub action pushes to branch db-export

## [0.5.2] - 2023-10-19

### Fixed

- Rename resource "filename" to "name" in metadata

## [0.5.1] - 2023-10-19

### Added

- Default fields for configurations files

## [0.5.0] - 2023-10-16

- Build views in pipeline instead of in hapi-schema

## [0.4.3] - 2023-10-13

### Fixed

- DB Export GitHub action runs on tag push

## [0.4.2] - 2023-10-12

### Fixed

- Pinned postgres docker image version in DB export GitHub action
- Change sector mapping for erl to ERY

## [0.4.1] - 2023-10-11

### Fixed

- Update requirements to use latest `hapi-schema`
- Change DB export GitHub action to have the HDX API key, and
  to run `pg_dump`  in the postgres docker container

## [0.4.0] - 2023-10-11

### Added

- GitHub Action to create DB export

## [0.3.0] - 2023-10-11

### Added

- Sector and org_type mappings

## [0.2.3] - 2023-10-10

### Fixed

- Remove duplicates from operational presence
- Org type module name from schemas library

## [0.2.2] - 2023-10-06

### Fixed

- Remove HDX link from org

## [0.2.1] - 2023-10-02

### Fixed

- Operational presence resource ref

## [0.2.1] - 2023-10-03

### Added

- Splitting of configs files

## [0.2.0] - 2023-09-29

### Added

- 3W data ingestion