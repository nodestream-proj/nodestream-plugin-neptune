from dataclasses import dataclass
from typing import Any, Dict, List

UNWIND_COMMIT_QUERY = """
UNWIND $params as param
"""


@dataclass(slots=True, frozen=True)
class Query:
    query_statement: str
    parameters: Dict[str, Any]

    @classmethod
    def from_statement(cls, statement: str):
        return cls(query_statement=statement, parameters={})


@dataclass(slots=True, frozen=True)
class QueryBatch:
    query_statement: str
    parameters: List[Dict[str, Any]]

    def as_query(self) -> Query:
        return Query(
            f"{UNWIND_COMMIT_QUERY}{self.query_statement}",
            {
                "params": self.parameters,
            },
        )
