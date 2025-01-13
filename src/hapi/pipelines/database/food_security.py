"""Functions specific to the food security theme."""

from dataclasses import dataclass
from logging import getLogger
from typing import Dict, List, Optional

from hapi_schema.db_food_security import DBFoodSecurity
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.location.adminlevel import AdminLevel
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import parse_date
from hdx.utilities.typehint import ListTuple
from sqlalchemy.orm import Session

from ..utilities.provider_admin_names import get_provider_name
from . import admins
from .base_uploader import BaseUploader
from .metadata import Metadata

logger = getLogger(__name__)


@dataclass
class AdminInfo:
    countryiso3: str
    name: str
    fullname: str
    pcode: Optional[str]
    exact: bool


class FoodSecurity(BaseUploader):
    def __init__(
        self,
        session: Session,
        metadata: Metadata,
        admins: admins.Admins,
        adminone: AdminLevel,
        admintwo: AdminLevel,
        countryiso3s: List[str],
        configuration: Configuration,
        error_handler: HDXErrorHandler,
    ):
        super().__init__(session)
        self._metadata = metadata
        self._admins = admins
        self._adminone = adminone
        self._admintwo = admintwo
        self._countryiso3s = countryiso3s
        self._configuration = configuration
        self._country_status = {}
        self._error_handler = error_handler

    @staticmethod
    def get_admin_level_from_resource_name(
        resource_name: str,
    ) -> Optional[str]:
        if "long_latest" not in resource_name:
            return None
        if "national" in resource_name:
            return "national"
        elif "level1" in resource_name:
            return "adminone"
        elif "area" in resource_name:
            return "admintwo"
        else:
            return None

    def get_adminoneinfo(
        self,
        adm_ignore_patterns: ListTuple,
        dataset_name: str,
        countryiso3: str,
        adminone_name: str,
    ) -> Optional[AdminInfo]:
        full_adm1name = f"{countryiso3}|{adminone_name}"
        if any(x in adminone_name.lower() for x in adm_ignore_patterns):
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 1: ignoring {full_adm1name}",
                message_type="warning",
            )
            return None
        pcode, exact = self._adminone.get_pcode(countryiso3, adminone_name)
        return AdminInfo(
            countryiso3, adminone_name, full_adm1name, pcode, exact
        )

    def get_admintwoinfo(
        self,
        adm_ignore_patterns: ListTuple,
        dataset_name: str,
        adminoneinfo: AdminInfo,
        admintwo_name: str,
    ) -> Optional[AdminInfo]:
        full_adm2name = (
            f"{adminoneinfo.countryiso3}|{adminoneinfo.name}|{admintwo_name}"
        )
        if any(x in admintwo_name.lower() for x in adm_ignore_patterns):
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 2: ignoring {full_adm2name}",
                message_type="warning",
            )
            return None
        pcode, exact = self._admintwo.get_pcode(
            adminoneinfo.countryiso3, admintwo_name, parent=adminoneinfo.pcode
        )
        return AdminInfo(
            adminoneinfo.countryiso3,
            admintwo_name,
            full_adm2name,
            pcode,
            exact,
        )

    def get_adminone_admin2_ref(
        self,
        food_sec_config: Dict,
        dataset_name: str,
        adminoneinfo: AdminInfo,
    ) -> Optional[int]:
        if not adminoneinfo.pcode:
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 1: could not match {adminoneinfo.fullname}!",
                message_type="warning",
            )
            return None
        if not adminoneinfo.exact:
            name = self._adminone.pcode_to_name[adminoneinfo.pcode]
            if adminoneinfo.name in food_sec_config["adm1_errors"]:
                self._error_handler.add_message(
                    "FoodSecurity",
                    dataset_name,
                    f"Admin 1: ignoring erroneous {adminoneinfo.fullname} match to {name} {(adminoneinfo.pcode)}!",
                )
                return None
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 1: matching {adminoneinfo.fullname} to {name} {(adminoneinfo.pcode)}",
                message_type="warning",
            )
        return self._admins.get_admin2_ref(
            "adminone",
            adminoneinfo.pcode,
            dataset_name,
            "FoodSecurity",
            self._error_handler,
        )

    def get_admintwo_admin2_ref(
        self,
        food_sec_config: Dict,
        dataset_name: str,
        row: Dict,
        adminoneinfo: AdminInfo,
    ) -> Optional[int]:
        admintwo_name = row["Area"]
        if not admintwo_name:
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 1: ignoring blank Area name in {adminoneinfo.countryiso3}|{adminoneinfo.name}",
                message_type="warning",
            )
            return None
        admintwoinfo = self.get_admintwoinfo(
            food_sec_config["adm_ignore_patterns"],
            dataset_name,
            adminoneinfo,
            admintwo_name,
        )
        if not admintwoinfo:
            return None
        if not admintwoinfo.pcode:
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 2: could not match {admintwoinfo.fullname}!",
                message_type="warning",
            )
            return None
        if not admintwoinfo.exact:
            name = self._admintwo.pcode_to_name[admintwoinfo.pcode]
            if admintwo_name in food_sec_config["adm2_errors"]:
                self._error_handler.add_message(
                    "FoodSecurity",
                    dataset_name,
                    f"Admin 2: ignoring erroneous {admintwoinfo.fullname} match to {name} {(admintwoinfo.pcode)}!",
                )
                return None
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 2: matching {admintwoinfo.fullname} to {name} {(admintwoinfo.pcode)}",
                message_type="warning",
            )
        return self._admins.get_admin2_ref(
            "admintwo",
            admintwoinfo.pcode,
            dataset_name,
            "FoodSecurity",
            self._error_handler,
        )

    def process_subnational(
        self,
        food_sec_config: Dict,
        dataset_name: str,
        countryiso3: str,
        admin_level: str,
        row: Dict,
    ) -> Optional[int]:
        # Some countries only have data in the ipc_global_level1 file
        if (
            admin_level == "admintwo"
            and countryiso3 in food_sec_config["adm1_only"]
        ):
            self._country_status[countryiso3] = (
                "Level 1: Admin 1, Area: ignored"
            )
            adminoneinfo = self.get_adminoneinfo(
                food_sec_config["adm_ignore_patterns"],
                dataset_name,
                countryiso3,
                row["Level 1"],
            )
            return self.get_adminone_admin2_ref(
                food_sec_config,
                dataset_name,
                adminoneinfo,
            )
        # The YAML configuration "adm2_only" specifies locations where
        # "Level 1" is not populated and "Area" is admin 2. (These are
        # exceptions since "Level 1" would normally be populated if "Area" is
        # admin 2.)
        if countryiso3 in food_sec_config["adm2_only"]:
            # Some countries only have data in the ipc_global_area file
            if admin_level == "adminone":
                return None
            adminoneinfo = AdminInfo(countryiso3, "NOT GIVEN", "", None, False)
            self._country_status[countryiso3] = (
                "Level 1: ignored, Area: Admin 2"
            )
            return self.get_admintwo_admin2_ref(
                food_sec_config,
                dataset_name,
                row,
                adminoneinfo,
            )

        if countryiso3 in food_sec_config["adm2_in_level1"]:
            row["Area"] = row["Level 1"]
            row["Level 1"] = None
            adminoneinfo = AdminInfo(countryiso3, "NOT GIVEN", "", None, False)
            self._country_status[countryiso3] = (
                "Level 1: Admin 2, Area: ignored"
            )
            return self.get_admintwo_admin2_ref(
                food_sec_config,
                dataset_name,
                row,
                adminoneinfo,
            )

        if countryiso3 in food_sec_config["adm1_in_area"]:
            if admin_level == "adminone":
                return None
            self._country_status[countryiso3] = (
                "Level 1: ignored, Area: Admin 1"
            )
            adminoneinfo = self.get_adminoneinfo(
                food_sec_config["adm_ignore_patterns"],
                dataset_name,
                countryiso3,
                row["Area"],
            )
            return self.get_adminone_admin2_ref(
                food_sec_config,
                dataset_name,
                adminoneinfo,
            )

        adminone_name = row["Level 1"]

        if not adminone_name:
            if admin_level == "adminone":
                if not adminone_name:
                    self._error_handler.add_message(
                        "FoodSecurity",
                        dataset_name,
                        f"Admin 1: ignoring blank Level 1 name in {countryiso3}",
                        message_type="warning",
                    )
                    return None
            else:
                # "Level 1" and "Area" are used loosely, so admin 1 or admin 2 data can
                # be in "Area". Usually if "Level 1" is populated, "Area" is admin 2
                # and if it isn't, "Area" is admin 1.
                adminone_name = row["Area"]
                if not adminone_name:
                    self._error_handler.add_message(
                        "FoodSecurity",
                        dataset_name,
                        f"Admin 1: ignoring blank Area name in {countryiso3}",
                        message_type="warning",
                    )
                    return None
                adminoneinfo = self.get_adminoneinfo(
                    food_sec_config["adm_ignore_patterns"],
                    dataset_name,
                    countryiso3,
                    adminone_name,
                )
                if not adminoneinfo:
                    return None
                self._country_status[countryiso3] = (
                    "Level 1: ignored, Area: Admin 1"
                )
                return self.get_adminone_admin2_ref(
                    food_sec_config,
                    dataset_name,
                    adminoneinfo,
                )

        adminoneinfo = self.get_adminoneinfo(
            food_sec_config["adm_ignore_patterns"],
            dataset_name,
            countryiso3,
            adminone_name,
        )
        if not adminoneinfo:
            return None
        if countryiso3 in food_sec_config["adm1_only"]:
            self._country_status[countryiso3] = (
                "Level 1: Admin 1, Area: ignored"
            )
        else:
            self._country_status[countryiso3] = (
                "Level 1: Admin 1, Area: Admin 2"
            )
        if admin_level == "adminone":
            return self.get_adminone_admin2_ref(
                food_sec_config, dataset_name, adminoneinfo
            )
        return self.get_admintwo_admin2_ref(
            food_sec_config,
            dataset_name,
            row,
            adminoneinfo,
        )

    def populate(self) -> None:
        logger.info("Populating food security table")
        reader = Read.get_reader("hdx")
        dataset = reader.read_dataset(
            "global-acute-food-insecurity-country-data", self._configuration
        )
        self._metadata.add_dataset(dataset)
        dataset_id = dataset["id"]
        dataset_name = dataset["name"]
        food_sec_config = self._configuration["food_security"]
        for resource in dataset.get_resources():
            admin_level = self.get_admin_level_from_resource_name(
                resource["name"]
            )
            if not admin_level:
                continue
            self._metadata.add_resource(dataset_id, resource)
            resource_id = resource["id"]
            url = resource["url"]
            headers, rows = reader.get_tabular_rows(url, dict_form=True)
            # Date of analysis,Country,Total country population,Level 1,Area,Validity period,From,To,Phase,Number,Percentage
            for row in rows:
                if "#" in row["Date of analysis"]:  # ignore HXL row
                    continue
                countryiso3 = row["Country"]
                if countryiso3 not in self._countryiso3s:
                    continue
                provider_admin1_name = get_provider_name(row, "Level 1")
                provider_admin2_name = get_provider_name(row, "Area")
                if admin_level == "national":
                    admin2_ref = self._admins.get_admin2_ref(
                        admin_level,
                        countryiso3,
                        dataset_name,
                        "FoodSecurity",
                        self._error_handler,
                    )
                else:
                    admin2_ref = self.process_subnational(
                        food_sec_config,
                        dataset_name,
                        countryiso3,
                        admin_level,
                        row,
                    )
                if not admin2_ref:
                    admin2_ref = self._admins.get_admin2_ref(
                        "national",
                        countryiso3,
                        "FoodSecurity",
                        dataset_name,
                        self._error_handler,
                    )
                time_period_start = parse_date(row["From"])
                time_period_end = parse_date(row["To"])

                food_security_row = DBFoodSecurity(
                    resource_hdx_id=resource_id,
                    admin2_ref=admin2_ref,
                    provider_admin1_name=provider_admin1_name,
                    provider_admin2_name=provider_admin2_name,
                    ipc_phase=row["Phase"],
                    ipc_type=row["Validity period"],
                    reference_period_start=time_period_start,
                    reference_period_end=time_period_end,
                    population_in_phase=row["Number"],
                    population_fraction_in_phase=row["Percentage"],
                )
                self._session.add(food_security_row)
            self._session.commit()
        logger.info(f"{dataset_name} - Country Status")
        for countryiso3 in sorted(self._country_status):
            status = self._country_status[countryiso3]
            logger.info(f"{countryiso3}: {status}")
