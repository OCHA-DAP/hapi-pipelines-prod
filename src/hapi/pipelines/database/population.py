"""Functions specific to the population theme."""

from logging import getLogger
from typing import Dict

from hapi_schema.db_population import DBPopulation
from hdx.utilities.dateparse import parse_date_range
from sqlalchemy.orm import Session

from ..utilities.batch_populate import batch_populate
from ..utilities.provider_admin_names import get_provider_name
from . import admins
from .base_uploader import BaseUploader
from .metadata import Metadata

logger = getLogger(__name__)


class Population(BaseUploader):
    def __init__(
        self,
        session: Session,
        metadata: Metadata,
        admins: admins.Admins,
        results: Dict,
    ):
        super().__init__(session)
        self._metadata = metadata
        self._admins = admins
        self._results = results

    def populate(self) -> None:
        logger.info("Populating population table")
        for dataset in self._results.values():
            population_rows = []
            for admin_level, admin_results in dataset["results"].items():
                resource_id = admin_results["hapi_resource_metadata"]["hdx_id"]
                hxl_tags = admin_results["headers"][1]
                values = admin_results["values"]
                admin_codes = values[0].keys()
                for admin_code in admin_codes:
                    admin2_code = admins.get_admin2_code_based_on_level(
                        admin_code=admin_code, admin_level=admin_level
                    )
                    for irow in range(len(values[0][admin_code])):
                        gender = values[hxl_tags.index("#gender")][admin_code][
                            irow
                        ]
                        age_range = values[hxl_tags.index("#age+range")][
                            admin_code
                        ][irow]
                        min_age = values[hxl_tags.index("#age+min")][
                            admin_code
                        ][irow]
                        max_age = values[hxl_tags.index("#age+max")][
                            admin_code
                        ][irow]
                        population = values[hxl_tags.index("#population")][
                            admin_code
                        ][irow]
                        reference_year = values[hxl_tags.index("#date+year")][
                            admin_code
                        ][irow]
                        time_period_range = parse_date_range(
                            reference_year, "%Y"
                        )
                        provider_admin1_name = get_provider_name(
                            values,
                            "#adm1+name",
                            hxl_tags,
                            admin_code,
                            irow,
                        )
                        provider_admin2_name = get_provider_name(
                            values,
                            "#adm2+name",
                            hxl_tags,
                            admin_code,
                            irow,
                        )
                        population_row = dict(
                            resource_hdx_id=resource_id,
                            admin2_ref=self._admins.admin2_data[admin2_code],
                            provider_admin1_name=provider_admin1_name,
                            provider_admin2_name=provider_admin2_name,
                            gender=gender,
                            age_range=age_range,
                            min_age=min_age,
                            max_age=max_age,
                            population=int(population),
                            reference_period_start=time_period_range[0],
                            reference_period_end=time_period_range[1],
                        )
                        population_rows.append(population_row)
            batch_populate(population_rows, self._session, DBPopulation)
