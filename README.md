# Neptune Plugin for Nodestream

This plugin provides a [Nodestream](https://github.com/nodestream-proj/nodestream) interface to Amazon Neptune.

It supports both Amazon Neptune Database and Amazon Neptune Analytics as targets, and provides:

- **Ingest** — write nodes and relationships into Neptune from any Nodestream pipeline.
- **Copy** — copy a graph between Neptune targets (e.g. Neptune Database → Neptune Analytics) with `nodestream copy`.

Requires `nodestream >= 0.16`.

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

### Copying between graphs

Copy nodes and relationships from one Neptune target to another:

```bash
nodestream copy --from my-neptune-db --to my-neptune-analytics --node Person --relationship KNOWS
```

Copy identity is derived from each type's key properties. To key on Neptune's
native element id, declare `~id` as the key in your schema.
