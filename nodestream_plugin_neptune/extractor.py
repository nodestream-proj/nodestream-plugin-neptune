import json
from logging import getLogger
from typing import Any, Dict, Optional

from nodestream.pipeline.extractors import Extractor

from .neptune_connector import NeptuneConnector
from .neptune_query_executor import NeptuneQueryExecutor


class NeptuneDBExtractor(Extractor):
    @classmethod
    def from_file_data(
        cls,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        **connector_args
    ):
        connector = NeptuneConnector.from_file_data(**connector_args)
        return cls(query, connector, parameters, limit)

    def __init__(
        self,
        query: str,
        connector: NeptuneConnector,
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
        executor: NeptuneQueryExecutor = self.connector.make_query_executor()

        params = dict(**self.parameters, limit=self.limit, offset=offset)
        self.logger.info(
            "Running query on Neptune Database",
            extra=dict(query=self.query, params=params),
        )

        response = await executor.query(self.query, json.dumps(params))

        returned_records = []
        if response:
            returned_records = list(response["results"])

        for item in returned_records:
            yield item
