
from dataclasses import dataclass, field
import datetime
import json
from typing import Dict
import xml.etree.cElementTree as ET

import requests

from legacy.abstract_partners import AbstractPartner, PartnerRecord
from legacy.config import PARTNER_A_SSP_LOGIN, PARTNER_B_SSP_LOGIN_DATA, PARTNER_M_SSP_ACCESS_TOKEN


@dataclass
class SSPPartnerM(AbstractPartner):
    urltype: str = 'json'
    id: str = 'ssp-partner-m'
    currency: str = 'rub'
    _headers: Dict[str, str] = field(default_factory=lambda: {'Authorization': f'Bearer {PARTNER_M_SSP_ACCESS_TOKEN}'})

    def get_urls(self, start_date, finish_date):
        return [f"https://ssp-partner-m.example/v2/statistics?date_from={start_date}&date_to={finish_date}",
                ]

    def norm_parse(self, text):
        _json = json.loads(text)
        rows = _json['items'][0]['rows']

        def extract_data(data):
            shows = int(float(data['base']['shows']))
            spent = float(data['base']['spent'])
            date = data['date']
            return PartnerRecord(date, shows, spent)

        return map(extract_data, rows)


@dataclass
class SSPPartnerB(AbstractPartner):
    urltype: str = 'json'
    id: str = 'ssp-partner-b'
    currency: str = 'rub'

    def authentificate(self):
        """Получаем токен и сохраняем его в headers."""
        response = requests.post(
            'https://ssp-partner-b.example/auth',
            data={
                'login': PARTNER_B_SSP_LOGIN_DATA['login'],
                'password': PARTNER_B_SSP_LOGIN_DATA['password']
            }
        )
        token = response.json()['data']
        self._headers['Authorization'] = f'Token {token}'

    def get_urls(self, start_date, finish_date):
        return [f'https://ssp-partner-b.example/users/{PARTNER_B_SSP_LOGIN_DATA["user_id"]}/report?start_date={start_date}&end_date={finish_date}',
                ]

    def norm_parse(self, text):
        _json = json.loads(text)
        total = _json['data']['total']
        dates = total['date']

        def extract_data(date):
            count_imps = int(float(total['count_imps'][date]))
            net_payable_data = float(total['net_payable_data'][date])
            return PartnerRecord(date, count_imps, net_payable_data)

        return map(extract_data, dates)


@dataclass
class SSPPartnerO(AbstractPartner):
    urltype: str = 'json'
    id: str = 'ssp-partner-o'

    def get_urls(self, start_date, finish_date):
        return [f'https://ssp-partner-o.example/reporting/dsp?start_date={start_date}&end_date={finish_date}',
                ]

    def norm_parse(self, text):
        _json = json.loads(text)
        rows = _json['data']

        def extract_data(data):
            impression = int(float(data['impressionCount']))
            spent = float(data['spent'])
            date = data['date']
            return PartnerRecord(date, impression, spent)

        return map(extract_data, rows)


@dataclass
class SSPPartnerS(AbstractPartner):
    urltype: str = 'json'
    id: str = 'ssp-partner-s'

    def get_urls(self, start_date, finish_date):
        return [f'https://ssp-partner-s.example/api/v1/dsp-report?campaign=display_eur&start={start_date}&end={finish_date}',
                f'https://ssp-partner-s.example/api/v1/dsp-report?campaign=display_us&start={start_date}&end={finish_date}',
                f'https://ssp-partner-s.example/api/v1/dsp-report?campaign=video_eur&start={start_date}&end={finish_date}',
                ]

    def norm_parse(self, text):
        _json = json.loads(text)
        items = _json.items()

        def extract_data(data):
            date_key, values = data
            impressions = int(float(values['impressions']))
            revenue = float(values['revenue'])
            return PartnerRecord(date_key, impressions, revenue)

        return map(extract_data, items)


@dataclass
class SSPPartnerC(AbstractPartner):
    urltype: str = 'xml'
    id: str = 'ssp-partner-c'

    def get_urls(self, start_date, finish_date):
        return [f'https://ssp-partner-c.example/dsp-report.xml?start={start_date}&end={finish_date}',
                ]

    def norm_parse(self, text):
        _xml = ET.fromstring(text)

        def extract_data(element):
            date = element.attrib['date']
            impressions = int(float(element.find('impressions').text))
            revenue = float(element.find('revenue').text)
            return PartnerRecord(date, impressions, revenue)

        return map(extract_data, _xml)


@dataclass
class SSPPartnerD(AbstractPartner):
    urltype: str = 'xml'
    id: str = 'ssp-partner-d'

    def get_urls(self, start_date, finish_date):
        return [f'https://ssp-partner-d.example/xml-report?format=xml&start={start_date}&end={finish_date}',
                f'https://ssp-partner-d.example/xml-report?format=xml&start={start_date}&end={finish_date}',
                f'https://ssp-partner-d.example/xml-report?format=xml&start={start_date}&end={finish_date}',
                ]

    def norm_parse(self, text):
        _xml = ET.fromstring(text)

        def extract_data(data):
            date = data.attrib['date']
            impressions = int(float(data.find('impressions').text))
            revenue = float(data.find('revenue').text)
            return PartnerRecord(date, impressions, revenue)

        return map(extract_data, _xml)


@dataclass
class SSPPartnerA(AbstractPartner):
    urltype: str = 'json'
    id: str = 'superpartner'

    def authentificate(self):
        """Получаем токен и сохраняем его в headers."""
        response = requests.post(
            "https://ssp-partner-a.example/oauth2/token",
            data={
                'grant_type': PARTNER_A_SSP_LOGIN['grant_type'],
                'client_id': PARTNER_A_SSP_LOGIN['client_id'],
                'username': PARTNER_A_SSP_LOGIN['username'],
                'password': PARTNER_A_SSP_LOGIN['password'],
            }
        )
        token = response.json()['access_token']
        self._headers['Authorization'] = f'Bearer {token}'

    def format_date(self, date: datetime):
        date_str = date.strftime("%Y%m%d")
        return date_str

    def get_urls(self, start_date, finish_date):
        return [
            f"https://superpartner.example/v1/api/report?token=verisecret&start_date={start_date}&end_date={finish_date}&group=date"
        ]

    def norm_parse(self, text):
        _json = json.loads(text)
        rows = _json.get('data', {})

        def extract_data(date_str):
            data = rows[date_str]
            shows = int(float(data.get('impression_count', 0)))
            spent = float(data.get('cost', 0))
            return PartnerRecord(date=date_str, imps=shows, spent=spent)

        return map(extract_data, rows)
