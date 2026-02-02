from dataclasses import dataclass, field
import datetime
from itertools import chain
import json
from typing import Dict
import xml.etree.cElementTree as ET

import httpx

from legacy.abstract_partners import AbstractPartner, PartnerRecord
from legacy.config import PARTNER_B_DSP_LOGIN_DATA, PARTNER_M_DSP_ACCESS_TOKEN


@dataclass
class DSPPartnerI(AbstractPartner):
    urltype: str = 'json'
    id: str = '71'

    def format_date(self, date: datetime):
        date_str = date.strftime("%Y%m%d")
        return date_str

    def get_urls(self, start_date, finish_date):
        return [f'https://dsp-partner-i.example/sspReport?start={start_date}&end={finish_date}',
                ]

    def norm_parse(self, text):
        _json = json.loads(text)
        items = _json['data']

        def extract_data(data):
            date = data['date']
            imps = int(float(data['imp']))
            revenue = float(data['revenue'])
            return PartnerRecord(date, imps, revenue)

        return map(extract_data, items)


@dataclass
class DSPPartnerO(AbstractPartner):
    urltype: str = 'json'
    id: str = '65'
    currency: str = 'rub'

    def get_urls(self, start_date, finish_date):
        return [f'https://dsp-partner-o.example/v1/reporting?start={start_date}&end={finish_date}&group=day',
                ]

    def norm_parse(self, text):
        _json = json.loads(text)
        rows = _json['data']

        def extract_data(data):
            date = f"{data['day'][6:11]}-{data['day'][3:5]}-{data['day'][0:2]}"
            impressions = int(float(data['impressions']))
            earnings = float(data['earnings']) / 1000
            return PartnerRecord(date, impressions, earnings)

        return map(extract_data, rows)


@dataclass
class DSPPartnerM(AbstractPartner):
    urltype: str = 'json'
    id: str = '27'
    currency: str = 'rub'
    _headers: Dict[str, str] = field(default_factory=lambda: {'Authorization': f'Bearer {PARTNER_M_DSP_ACCESS_TOKEN}'})

    def get_urls(self, start_date, finish_date):
        return [f'https://dsp-partner-m.example/api/v2/statistics?date_from={start_date}&date_to={finish_date}',
                ]

    def norm_parse(self, text):
        _json = json.loads(text)
        items = _json['items']

        def extract_data(data):
            rows = data['rows']

            def extract_row(row):
                date = row['date']
                shows = int(float(row['shows']))
                amount = float(row['amount'])
                return PartnerRecord(date, shows, amount)

            return map(extract_row, rows)

        return chain.from_iterable(map(extract_data, items))


@dataclass
class DSPPartnerB(AbstractPartner):
    urltype: str = 'json'
    id: str = '35'
    currency: str = 'rub'

    async def authentificate(self):
        """Получаем токен и сохраняем его в headers."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'https://dsp-partner-b.example/token',
                data={
                    'login': PARTNER_B_DSP_LOGIN_DATA['login'],
                    'password': PARTNER_B_DSP_LOGIN_DATA['password']
                }
            )
            token = response.json()['data']
            self._headers['Authorization'] = f'Token {token}'

    def get_urls(self, start_date, finish_date):
        return [f'https://dsp-partner-b.example/users/{PARTNER_B_DSP_LOGIN_DATA["user_id"]}/sites/chart?start_date={start_date}&end_date={finish_date}',
                ]

    def norm_parse(self, text):
        _json = json.loads(text)
        total = _json['data']['total']
        dates = total['date']

        def extract_data(data):
            count_imps = int(float(total['count_imps'][data]))
            total_pub_payable = float(total['total_pub_payable'][data])
            return PartnerRecord(data, count_imps, total_pub_payable)

        return map(extract_data, dates)


@dataclass
class DSPPartnerF(AbstractPartner):
    urltype: str = 'xml'
    id: str = '110'

    def get_urls(self, start_date, finish_date):
        return [f'https://dsp-partner-f.example/ssp_xml?start={start_date}&end={finish_date}',
                f'https://dsp-partner-f.example/ssp_xml?start={start_date}&end={finish_date}',
                ]

    def norm_parse(self, text):
        _xml = ET.fromstring(text)

        def extract_data(data):
            date = data.find('date').text
            impressions = int(float(data.find('impressions').text))
            revenue = float(data.find('revenue').text)
            return PartnerRecord(date, impressions, revenue)

        return map(extract_data, _xml)
