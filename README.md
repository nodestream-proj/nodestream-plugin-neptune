# Neptune Plugin for Nodestream

This plugin provides a [Nodestream](https://github.com/nodestream-proj/nodestream) interface to Amazon Neptune. 

## Installation

```bash
pip install nodestream-plugin-neptune
```

## Usage

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
```
