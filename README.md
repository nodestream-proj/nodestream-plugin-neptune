# Neptune Plugin for Nodestream

This plugin provides a [Nodestream](https://github.com/nodestream-proj/nodestream) interface to Amazon Neptune. 

**NOTE: This plugin is currently in development and is not yet ready for production use.**

## Installation

```bash
pip install nodestream-plugin-neptune
```

**NOTE:** This plugin is currently only available in a pre-release version. When creating a new project with
`nodestream new test_project_name --database neptune`, you may need to update the generated pyproject.toml file to
use the latest pre-release version of the Neptune plugin from PyPI. For example:

```toml
# ...
[tool.poetry.dependencies]
python = "^3.10"
nodestream = "^0.12.0a2"

nodestream-plugin-neptune = "^0.12"
# ...
```
should be updated to
```toml
# ...
[tool.poetry.dependencies]
python = "^3.10"
nodestream = "^0.12.0a3"
nodestream-plugin-neptune = "^0.12.0a2"
# ...
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
