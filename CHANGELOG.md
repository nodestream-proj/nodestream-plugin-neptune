# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Playground `types` pipeline (`playground_project/types.yaml`) that writes every
  value type the plugin can send to Neptune, on nodes and relationships, with
  edge cases (empty, unicode and special-character strings, zero, negative and
  64-bit-boundary integers, whole, scientific and near-limit floats, booleans,
  null, and a datetime), so a target's stored values can be read back and
  compared.

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
- On copy, a source node's native `~id` is used only as key material. When the
  schema keys on anything else it is now dropped instead of being left in the
  node's properties, where Neptune Database rejected the whole write batch
  (`'~id' as a property in SET clause is not supported`) and Neptune Analytics
  and Neo4j stored it as a literal `~id` property.
- A warning is logged, once per node type, when a declared key property is not
  stored on the source nodes; nodestream skips such nodes without a trace
  otherwise.

### Fixed

- Neptune Analytics can now be used as a source for `nodestream copy`. Its
  `execute_query` returns results only inside a streaming `payload`, which was
  never read (and was closed by `execute()`), so every read looked empty: the
  type histogram counted zero for every type, no extractors were built, and the
  copy finished immediately with exit code 0 having copied nothing. The
  payload is now decoded into `results`, matching Neptune Database.
- The playground project's `test` pipeline failed to load ("Failed to Initialize
  Component in Declarative Pipeline") because it used the `FileExtractor`
  argument form removed in nodestream 0.13; it now uses `sources:`. The
  playground targets also set `region`.

## [0.14.0] - Initial documented release

> Full history prior to this release is available in the [git log](https://github.com/nodestream-proj/nodestream-plugin-neptune/commits/main).
