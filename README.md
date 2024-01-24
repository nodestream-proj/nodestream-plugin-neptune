# Neptune Plugin for Nodestream

This plugin provides a [Nodestream](https://github.com/nodestream-proj/nodestream) interface to Amazon Neptune. 

**NOTE: This plugin is currently in development and is not yet ready for production use.**

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
    # ... TODO: Add documentation of neptune connection args.
```
