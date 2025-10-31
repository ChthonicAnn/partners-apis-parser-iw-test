from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import datetime
import httpx
from typing import Any, Dict, List


@dataclass
class PartnerRecord:
    date: datetime
    imps: int
    spent: float


@dataclass
class AbstractPartner(ABC):
    urltype: str
    id: str
    currency: str = 'usd'
    _headers: Dict[str, str] = field(default_factory=dict)

    async def authentificate(self) -> None:
        pass

    def format_date(self, date: datetime) -> Any:
        date_str = date.strftime("%Y-%m-%d")
        return date_str

    @abstractmethod
    def get_urls(self, start_date, finish_date) -> List[Any]:
        pass

    async def request_data(self, url) -> str:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self._headers)
            return response.text

    @abstractmethod
    def norm_parse(self, text: str) -> List[PartnerRecord]:
        pass
