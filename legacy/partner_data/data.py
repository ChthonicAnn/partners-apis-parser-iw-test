from dataclasses import dataclass
import datetime


@dataclass
class PartnerData:
    date: datetime.date
    dsp_id: int = 0
    ssp: str = ''
    imps: int = 0
    spent: float = 0
    currency: str = 'usd'
