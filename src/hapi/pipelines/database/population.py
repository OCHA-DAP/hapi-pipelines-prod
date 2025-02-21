"""Functions specific to the population theme."""

from logging import getLogger
from typing import Dict

from hapi_schema.db_population import DBPopulation

from .hapi_dataset_uploader import HapiDatasetUploader

logger = getLogger(__name__)


class Population(HapiDatasetUploader):
    def populate_row(self, output_row: Dict, row: Dict) -> None:
        output_row["gender"] = row["gender"]
        output_row["age_range"] = row["age_range"]
        output_row["min_age"] = (
            int(float(row["min_age"])) if row["min_age"] else None
        )
        output_row["max_age"] = (
            int(float(row["max_age"])) if row["max_age"] else None
        )
        output_row["population"] = int(row["population"])

    def populate(self) -> None:
        self.hapi_populate(
            "population",
            DBPopulation,
            end_resource=2,
        )
