from logging import getLogger

from nodestream.schema.migrations import Migrator
from nodestream.schema.migrations.operations import Operation


class NeptuneMigrator(Migrator):
    def __init__(self):
        self.logger = getLogger(__name__)

    async def execute_operation(self, _: Operation) -> None:
        self.logger.warning(
            "Migrations are not currently supported for Neptune. Skipping operation."
        )
