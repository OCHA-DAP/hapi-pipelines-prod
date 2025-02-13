"""Functions specific to the conflict event theme."""

from logging import getLogger

from hapi_schema.db_conflict_event import DBConflictEvent
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import invert_dictionary
from sqlalchemy.orm import Session

from ..utilities.batch_populate import batch_populate
from . import admins
from .base_uploader import BaseUploader
from .metadata import Metadata

logger = getLogger(__name__)


class ConflictEvent(BaseUploader):
    def __init__(
        self,
        session: Session,
        metadata: Metadata,
        admins: admins.Admins,
        configuration: Configuration,
        error_handler: HDXErrorHandler,
    ):
        super().__init__(session)
        self._metadata = metadata
        self._admins = admins
        self._configuration = configuration
        self._error_handler = error_handler

    def populate(self) -> None:
        logger.info("Populating conflict event table")
        reader = Read.get_reader("hdx")
        dataset = reader.read_dataset(
            "hdx-hapi-conflict-event", self._configuration
        )
        resources = dataset.get_resources()
        for resource in resources:
            url = resource["url"]
            headers, rows = reader.get_tabular_rows(url, dict_form=True)
            hxltag_to_header = invert_dictionary(next(rows))
            conflict_event_rows = []

            for row in rows:
                if row["error"]:
                    continue
                resource_id = row["resource_hdx_id"]
                dataset_id = row["dataset_hdx_id"]
                dataset_name = self._metadata.get_dataset_name(dataset_id)
                resource_name = self._metadata.get_resource_name(resource_id)
                if not resource_name:
                    dataset = reader.read_dataset(
                        dataset_id, self._configuration
                    )
                    for r in dataset.get_resources():
                        if r["id"] == resource_id:
                            self._metadata.add_dataset(dataset)
                            self._metadata.add_resource(dataset_id, r)

                admin_level = self._admins.get_admin_level_from_row(
                    hxltag_to_header, row, 2
                )
                admin2_ref = self._admins.get_admin2_ref_from_row(
                    hxltag_to_header,
                    row,
                    dataset_name,
                    "ConflictEvent",
                    admin_level,
                )
                provider_admin1_name = row["provider_admin1_name"] or ""
                provider_admin2_name = row["provider_admin2_name"] or ""

                conflict_event_row = {
                    "resource_hdx_id": resource_id,
                    "admin2_ref": admin2_ref,
                    "provider_admin1_name": provider_admin1_name,
                    "provider_admin2_name": provider_admin2_name,
                    "event_type": row["event_type"],
                    "fatalities": row["fatalities"],
                    "reference_period_start": parse_date(
                        row["reference_period_start"]
                    ),
                    "reference_period_end": parse_date(
                        row["reference_period_end"],
                        max_time=True,
                    ),
                }
                conflict_event_rows.append(conflict_event_row)

            batch_populate(conflict_event_rows, self._session, DBConflictEvent)
