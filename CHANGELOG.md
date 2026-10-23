# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Ported the `nodestream copy` surface to the nodestream 0.16.x `TypeRetriever`
  API (`build_histogram`/`fetch_extractors`, with mapping moved into typed
  per-type extractors). Node and relationship identity is now derived from the
  schema's key properties, so a copy schema can key on the native Neptune `~id`.
  The minimum supported `nodestream` is now `^0.16.0` (was `^0.15.0`).
- On copy, a source relationship's native `~id` is preserved as the plain edge
  property `neptuneEdgeId` (the same name Neptune Analytics' import uses for
  preserved edge ids) instead of a literal `~id` property. Neptune Analytics
  does not support custom edge ids and assigns its own.

## [0.14.0] - Initial documented release

> Full history prior to this release is available in the [git log](https://github.com/nodestream-proj/nodestream-plugin-neptune/commits/main).
