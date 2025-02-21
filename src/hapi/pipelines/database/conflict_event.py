"""Functions specific to the conflict event theme."""

from logging import getLogger
from typing import Dict

from hapi_schema.db_conflict_event import DBConflictEvent

from .hapi_dataset_uploader import HapiDatasetUploader

logger = getLogger(__name__)


class ConflictEvent(HapiDatasetUploader):
    def populate_row(self, output_row: Dict, row: Dict) -> None:
        output_row["event_type"] = row["event_type"]
        output_row["events"] = row["events"]
        output_row["fatalities"] = row["fatalities"]

    def populate(self) -> None:
        self.hapi_populate(
            "conflict-event",
            DBConflictEvent,
            end_resource=None,
        )
