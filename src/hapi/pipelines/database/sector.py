import logging
from typing import Dict
from unicodedata import normalize

from hapi_schema.db_sector import DBSector
from hdx.scraper.utilities.reader import Read
from hdx.utilities.dateparse import parse_date
from sqlalchemy.orm import Session

from .base_uploader import BaseUploader

logger = logging.getLogger(__name__)


class Sector(BaseUploader):
    def __init__(
        self,
        session: Session,
        datasetinfo: Dict[str, str],
        sector_map: Dict[str, str],
    ):
        super().__init__(session)
        self._datasetinfo = datasetinfo
        self.data = {}
        self._sector_map = sector_map

    def populate(self):
        logger.info("Populating sector table")
        reader = Read.get_reader()
        headers, iterator = reader.read(self._datasetinfo)
        for row in iterator:
            code = row["#sector +code +acronym"]
            name = row["#sector +name +preferred +i_en"]
            date = parse_date(row["#date +created"])
            self.data[name] = code
            self.data[code] = code
            sector_row = DBSector(
                code=code,
                name=name,
                reference_period_start=date,
            )
            self._session.add(sector_row)
        self._session.commit()

    def get_sector_code(self, sector: str) -> str:
        # TODO: implement fuzzy matching of sector names/codes (HAPI-193)
        sector_code = self.data.get(sector)
        if sector_code:
            return sector_code
        sector = (
            normalize("NFKD", sector).encode("ascii", "ignore").decode("ascii")
        )
        sector_code = self._sector_map.get(sector.lower())
        return sector_code