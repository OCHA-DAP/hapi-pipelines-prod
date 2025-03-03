"""Functions specific to the funding theme."""

from logging import getLogger
from typing import Dict

from hapi_schema.db_funding import DBFunding

from .hapi_dataset_uploader import HapiDatasetUploader

logger = getLogger(__name__)


class Funding(HapiDatasetUploader):
    def populate_row(self, output_row: Dict, row: Dict) -> None:
        output_row["appeal_code"] = row["appeal_code"]
        output_row["appeal_name"] = row["appeal_name"]
        output_row["appeal_type"] = row["appeal_type"]
        output_row["requirements_usd"] = row["requirements_usd"]
        output_row["funding_usd"] = row["funding_usd"]
        output_row["funding_pct"] = row["funding_pct"]

    def populate(self) -> None:
        self.hapi_populate(
            "funding",
            DBFunding,
            max_admin_level=0,
        )
