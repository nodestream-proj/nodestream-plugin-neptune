# Neptune Plugin for Nodestream

This plugin provides a [Nodestream](https://github.com/nodestream-proj/nodestream) interface to Amazon Neptune.

It supports both Amazon Neptune Database and Amazon Neptune Analytics as targets, and provides:

- **Ingest** - write nodes and relationships into Neptune from any Nodestream pipeline.
- **Copy** - copy a graph between Neptune targets (e.g. Neptune Database → Neptune Analytics), or from Neptune to another Nodestream database such as Neo4j, with `nodestream copy`.

Requires `nodestream >= 0.16, < 0.17`.

## Installation

```bash
pip install nodestream-plugin-neptune
```

## Usage

Define Neptune targets in your `nodestream.yaml`:

```yaml
# nodestream.yaml
targets:
  my-neptune-db:
    database: neptune
    mode: database
    host: https://<NEPTUNE_ENDPOINT>:<PORT>
    region: <AWS_REGION>
  my-neptune-analytics:
    database: neptune
    mode: analytics
    graph_id: <GRAPH_IDENTIFIER>
    region: <AWS_REGION>
```

### Property values

Property values are sent to Neptune as openCypher parameters. The plugin writes
`str`, `bool`, `int`, `float` and `None` values as they are, and a `datetime` as
POSIX seconds (a float). Anything else (for example `Decimal`, `date`,
`timedelta`, `bytes`, a `dict`, or a database driver's own temporal, spatial or
vector type) raises a `TypeError` naming the property and node or relationship
type, instead of failing later inside the connection. `NaN` and infinite floats
are rejected too. Convert such values in your pipeline before they reach the
writer.

Neptune openCypher cannot store a list as a property value. A multi-valued
property is written as its first value and a warning is logged for every
occurrence, so the loss is visible; an empty list is dropped.

### Node keys

A node's identity is the native Neptune `~id`, built from the node type and its
key values (`Label_key:value`, keys sorted). Each key value is also written to
the node as an ordinary property, so it can be queried (`MATCH (n:Person
{first_name: 'Zach'})`) and used when copying. If a declared property has the
same name as a key, the property wins, which keeps the original value of a key
that nodestream normalized (by default string keys are lowercased); identity
still uses the normalized key. Relationship key values are written as
properties too.

The native `~id` is never written as a property: Neptune Database rejects it,
and Neptune Analytics would store a literal property named `~id`. It is dropped
with a warning if a pipeline tries to write it.

Nodes written by plugin versions before 0.16.2 kept their key only inside the
`~id`. Those nodes gain the key properties the next time they are written; until
then, copy them out by keying on `~id` (see below).

### Copying between graphs

Copy nodes and relationships from one Neptune target to another:

```bash
nodestream copy --from my-neptune-db --to my-neptune-analytics --node Person --relationship KNOWS
```

Copy identity is derived from each type's key properties. To key on Neptune's
native element id, declare `~id` as the key in your schema.

A few things follow from how Neptune stores keys and ids:

- The native `~id` is used only as a key. If you key on anything else it is not
  copied as a property, because Neptune Database cannot write `~id` as a
  property.
- If a declared key is not stored as a property on the source nodes, a warning
  is logged once for that node type and nodestream skips (or only partially
  identifies) those nodes. This applies to nodes written before 0.16.2 (see
  above); key those types on `~id` instead.
- A relationship's native id is preserved as the property `neptuneEdgeId`.
  Neptune Analytics does not support custom edge ids and assigns its own.
- Reads page through each type with `SKIP`/`LIMIT` (default page size 10,000).
  For large types, a warning suggests a larger page size to use with
  `--retriever-option limit=<n>`.

### Known limitations

- **Neptune Database as a copy source is verified on engine version 1.4.6.3.**
  To keep reads fast, the plugin pages through nodes and relationships with
  `SKIP`/`LIMIT` and no `ORDER BY`. On engine versions 1.4.7.0, 1.4.8.0, 1.4.8.1
  and 1.5.0.0, unordered `SKIP`/`LIMIT` can skip and duplicate rows, so a copy can
  silently be incomplete. The plugin does not yet detect these versions or
  enforce ordering ([#47](https://github.com/nodestream-proj/nodestream-plugin-neptune/issues/47)).
  When copying out of Neptune Database, compare the node and relationship counts
  of the source and the target. Neptune Analytics is not affected.
- **Neo4j → Neptune copy does not work yet.** The Neo4j plugin passes its driver
  types (such as `neo4j.time.DateTime`) through unchanged, including the
  `last_ingested_at` timestamps nodestream adds to every node, and this plugin
  refuses to write them. See
  [nodestream-plugin-neo4j#50](https://github.com/nodestream-proj/nodestream-plugin-neo4j/issues/50).
  Copying from Neptune to Neo4j, and between Neptune Database and Neptune
  Analytics, works.
- **A copy can change node ids.** Copy rebuilds each node's `~id` from its key
  properties, which keep their original case, while the pipeline that wrote the
  source lowercased string keys by default. The copy's `~id`s can then differ
  from the source's, and running the original pipeline against the copy creates
  duplicate nodes. Use `key_normalization: {do_lowercase_strings: false}` in the
  source pipelines, or treat the copy as a separate graph. See
  [#45](https://github.com/nodestream-proj/nodestream-plugin-neptune/issues/45).
- **Copying into an empty target can drop relationships.** `--preload-nodes`
  does not make relationship writes wait for node writes. Relationships whose
  endpoints are not written yet are dropped silently. Copy the nodes first and
  the relationships in a second `nodestream copy`. See
  [nodestream#466](https://github.com/nodestream-proj/nodestream/issues/466).
- **Server errors are logged, not raised.** If a write still fails after its
  retries (for example repeated `ConflictException` on Neptune Analytics), the
  batch is logged and skipped and the run continues. See
  [#46](https://github.com/nodestream-proj/nodestream-plugin-neptune/issues/46).
