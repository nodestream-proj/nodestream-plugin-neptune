# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.16.2] - 2026-10-30

Supports nodestream `>=0.16.0,<0.17.0` (tested against 0.16.0 and 0.16.2).

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
- The default read page size for copy is now 10,000 (was 100). When a type needs
  many pages, a warning names the `--retriever-option limit=<n>` to use, because
  Neptune Database's offset pagination gets slower with depth.
- Node key values are now also written as node properties. They were only
  encoded in the composite `~id`, so key-only values could not be queried and
  nodes could not be copied out of Neptune by their keys. Existing nodes gain the
  key properties the next time they are written. A declared property with the
  same name as a key takes precedence, which keeps the original value of a
  normalized key. A key named `~id` is never written as a property.
- On copy, a source relationship's native `~id` is preserved as the plain edge
  property `neptuneEdgeId` (the same name Neptune Analytics' import uses for
  preserved edge ids) instead of a literal `~id` property. Neptune Analytics
  does not support custom edge ids and assigns its own.
- On copy, a source node's native `~id` is used only as key material. When the
  schema keys on anything else it is now dropped instead of being left in the
  node's properties, where Neptune Database rejected the whole write batch
  (`'~id' as a property in SET clause is not supported`) and Neptune Analytics
  and Neo4j stored it as a literal `~id` property.
- The native `~id` is never written as a node or relationship property, for any
  source or pipeline. It is dropped with a warning (once per name and type).
- A warning is logged, once per node type, when a declared key property is not
  stored on the source nodes; nodestream skips such nodes without a trace
  otherwise.
- Multi-valued (list or tuple) property values are collapsed to their first
  value, with a warning for every occurrence, because Neptune openCypher cannot
  write array values. Before, the whole write batch failed and its nodes and
  relationships were dropped. An empty list drops the property.
- A property value the plugin cannot write (for example `Decimal`, `date`,
  `timedelta`, `bytes`, a `dict`, or a database driver's own type such as
  `neo4j.time.DateTime`) now raises a `TypeError` naming the property and the
  node or relationship type, before any query is sent. Before, the batch failed
  inside the connection, was logged, and the run exited 0 with the data missing.
- The boto client's connection pool is now 64 connections (the botocore default
  is 10), so many concurrent flush lanes no longer queue for a connection. A
  `config` passed in the target's client arguments is respected: only its pool
  size is raised, never lowered.
- The boto client is now created exactly once when many queries start at the
  same time, instead of once per query in the first burst.

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
