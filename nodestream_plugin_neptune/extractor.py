from logging import getLogger
from typing import Any, Dict, Optional

import json

from nodestream.pipeline.extractors import Extractor
from .database_connector import NeptuneDatabaseConnector


class NeptuneDBExtractor(Extractor):
    @classmethod
    def from_file_data(
        cls,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        **connector_args
    ):
        connector = NeptuneDatabaseConnector.from_file_data(**connector_args)
        return cls(query, connector, parameters, limit)

    def __init__(
        self,
        query: str,
        connector: NeptuneDatabaseConnector,
        parameters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
    ) -> None:
        self.connector = connector
        self.query = query
        self.parameters = parameters or {}
        self.limit = limit
        self.logger = getLogger(self.__class__.__name__)

    async def extract_records(self):
        # TODO: In the future, we should extract the database pagination logic from
        # this class and move it to a GraphDatabaseExtractor class following the lead
        # we have of the writer class.
        offset = 0
        should_continue = True
        client = self.connector.client

        params = dict(**self.parameters, limit=self.limit, offset=offset)
        self.logger.info(
            "Running query on Neptune Database",
            extra=dict(query=self.query, params=params),
        )
        query_results = client.execute_open_cypher_query(
            openCypherQuery = self.query,
            parameters = json.dumps(params),
        )
        returned_records = list(query_results['results'])
        for item in returned_records:
            yield item
