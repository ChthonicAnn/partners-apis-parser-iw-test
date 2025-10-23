import builtins
import inspect
from colorama import Fore, Style
from pandas import Timestamp
import json

from legacy.partner_data.data import PartnerData
from legacy.upd_parser import load_insert_data


# Все тестовые данные по URL в одном словаре
TEST_GET_RESPONSES = {
    # ---------- SSP ----------
    # ssp-partner-m
    "https://ssp-partner-m.example/v2/statistics?date_from=2025-01-01&date_to=2025-03-01": {
        "items": [
            {
                "rows": [
                    {"date": "2025-01-01", "base": {"shows": 1000, "spent": 10.5}},
                    {"date": "2025-01-02", "base": {"shows": 1500, "spent": 15.75}},
                ]
            }
        ]
    },

    # ssp-partner-b
    "https://ssp-partner-b.example/users/12345678/report?start_date=2025-01-01&end_date=2025-03-01": {
        "data": {
            "total": {
                "date": ["2025-01-01", "2025-01-02"],
                "count_imps": {"2025-01-01": 1200, "2025-01-02": 1800},
                "net_payable_data": {"2025-01-01": 12.3, "2025-01-02": 18.7},
            }
        }
    },

    # ssp-partner-o
    "https://ssp-partner-o.example/reporting/dsp?start_date=2025-01-01&end_date=2025-03-01": {
        "data": [
            {"date": "2025-01-01", "impressionCount": 2000, "spent": 20.5},
            {"date": "2025-01-02", "impressionCount": 2500, "spent": 25.0},
        ]
    },

    # ssp-partner-s (три кампании)
    "https://ssp-partner-s.example/api/v1/dsp-report?campaign=display_eur&start=2025-01-01&end=2025-03-01": {
        "2025-01-01": {"impressions": 1000, "revenue": 10.0},
        "2025-01-02": {"impressions": 1200, "revenue": 12.5},
    },
    "https://ssp-partner-s.example/api/v1/dsp-report?campaign=display_us&start=2025-01-01&end=2025-03-01": {
        "2025-01-01": {"impressions": 800, "revenue": 9.0},
        "2025-01-02": {"impressions": 950, "revenue": 10.2},
    },
    "https://ssp-partner-s.example/api/v1/dsp-report?campaign=video_eur&start=2025-01-01&end=2025-03-01": {
        "2025-01-01": {"impressions": 400, "revenue": 5.0},
        "2025-01-02": {"impressions": 500, "revenue": 6.2},
    },

    # ssp-partner-c (XML)
    "https://ssp-partner-c.example/dsp-report.xml?start=2025-01-01&end=2025-03-01":
        """
        <root>
            <day date="2025-01-01">
                <impressions>1000</impressions>
                <revenue>8.5</revenue>
            </day>
            <day date="2025-01-02">
                <impressions>1500</impressions>
                <revenue>12.0</revenue>
            </day>
        </root>
        """,

    # ssp-partner-d (XML, трижды вызывается)
    "https://ssp-partner-d.example/xml-report?format=xml&start=2025-01-01&end=2025-03-01":
        """
        <root>
            <day date="2025-01-01">
                <impressions>2000</impressions>
                <revenue>18.0</revenue>
            </day>
            <day date="2025-01-02">
                <impressions>2300</impressions>
                <revenue>20.0</revenue>
            </day>
        </root>
        """,

    # ---------- DSP ----------
    # dsp-partner-i
    "https://dsp-partner-i.example/sspReport?start=20250101&end=20250301": {
        "data": [
            {"date": "2025-01-01", "imp": 1000, "revenue": 10.5},
            {"date": "2025-01-02", "imp": 1500, "revenue": 15.0},
        ]
    },

    # dsp-partner-o
    "https://dsp-partner-o.example/v1/reporting?start=2025-01-01&end=2025-03-01&group=day": {
        "data": [
            {"day": "01-01-2025", "impressions": 1100, "earnings": 12000},
            {"day": "02-01-2025", "impressions": 1400, "earnings": 18000},
        ]
    },

    # dsp-partner-m
    "https://dsp-partner-m.example/api/v2/statistics?date_from=2025-01-01&date_to=2025-03-01": {
        "items": [
            {
                "rows": [
                    {"date": "2025-01-01", "shows": 2000, "amount": 21.0},
                    {"date": "2025-01-02", "shows": 2300, "amount": 25.5},
                ]
            }
        ]
    },


    # dsp-partner-b
    "https://dsp-partner-b.example/users/12345678/sites/chart?start_date=2025-01-01&end_date=2025-03-01": {
        "data": {
            "total": {
                "date": ["2025-01-01", "2025-01-02"],
                "count_imps": {"2025-01-01": 1400, "2025-01-02": 1600},
                "total_pub_payable": {"2025-01-01": 14.2, "2025-01-02": 16.8},
            }
        }
    },

    # dsp-partner-f (XML, два вызова)
    "https://dsp-partner-f.example/ssp_xml?start=2025-01-01&end=2025-03-01":
        """
        <root>
            <report>
                <date>2025-01-01</date>
                <impressions>900</impressions>
                <revenue>9.9</revenue>
            </report>
            <report>
                <date>2025-01-02</date>
                <impressions>1200</impressions>
                <revenue>12.5</revenue>
            </report>
        </root>
        """,
}

# Фиктивные ответы POST-запросов SSP и DSP партнёров
TEST_POST_RESPONSES = {
    # PARTNER_B_SSP_TOKEN
    "https://ssp-partner-b.example/auth": json.dumps({
        "data": "SSP_PARTNER_B_TOKEN"
    }),
    # PARTNER_B_DSP_TOKEN
    "https://dsp-partner-b.example/token": json.dumps({
        "data": "DSP_PARTNER_B_TOKEN"
    }),
    # PARTNER_A_SSP_TOKEN
    "https://ssp-partner-a.example/oauth2/token": {
        "access_token": "SSP_PARTNER_A_ACCESS_TOKEN"
    },
}


EXPECTED_RESULT = [
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=0, ssp='ssp-partner-m', imps=1000, spent=10.5, currency='rub'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=0, ssp='ssp-partner-m', imps=1500, spent=15.75, currency='rub'),
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=0, ssp='ssp-partner-b', imps=1200, spent=12.3, currency='rub'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=0, ssp='ssp-partner-b', imps=1800, spent=18.7, currency='rub'),
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=0, ssp='ssp-partner-o', imps=2000, spent=20.5, currency='usd'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=0, ssp='ssp-partner-o', imps=2500, spent=25.0, currency='usd'),
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=0, ssp='ssp-partner-s', imps=2200, spent=24.0, currency='usd'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=0, ssp='ssp-partner-s', imps=2650, spent=28.9, currency='usd'),
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=0, ssp='ssp-partner-c', imps=1000, spent=8.5, currency='usd'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=0, ssp='ssp-partner-c', imps=1500, spent=12.0, currency='usd'),
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=0, ssp='ssp-partner-d', imps=6000, spent=54.0, currency='usd'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=0, ssp='ssp-partner-d', imps=6900, spent=60.0, currency='usd'),
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=71, ssp='', imps=1000, spent=10.5, currency='usd'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=71, ssp='', imps=1500, spent=15.0, currency='usd'),
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=65, ssp='', imps=1100, spent=12.0, currency='rub'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=65, ssp='', imps=1400, spent=18.0, currency='rub'),
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=27, ssp='', imps=2000, spent=21.0, currency='rub'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=27, ssp='', imps=2300, spent=25.5, currency='rub'),
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=35, ssp='', imps=1400, spent=14.2, currency='rub'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=35, ssp='', imps=1600, spent=16.8, currency='rub'),
    PartnerData(date=Timestamp('2025-01-01 00:00:00'), dsp_id=110, ssp='', imps=1800, spent=19.8, currency='usd'),
    PartnerData(date=Timestamp('2025-01-02 00:00:00'), dsp_id=110, ssp='', imps=2400, spent=25.0, currency='usd')
]

# --- Перехват вызовов print из тестируемой функции, чтобы подставить имя функции и красить в разные цвета ---
original_print = builtins.print


def tagged_print(*args, **kwargs):
    stack = inspect.stack()
    # Caller of print
    if len(stack) > 1:
        caller = stack[1].function
    else:
        caller = "?"
    # If print called from test(), highlight as TEST
    if caller.startswith("test"):
        prefix = f"{Fore.CYAN}[TEST]{Style.RESET_ALL}"
    else:
        prefix = f"{Fore.YELLOW}[{caller}]{Style.RESET_ALL}"
    original_print(prefix, *args, **kwargs)


def test_load_insert_data(mocker):
    '''Функция для тестирования всего ебучего парсера.'''

    # Для красивеньких принтов. Перед сдачей удалить нахуй
    print()
    builtins.print = tagged_print

    # Arrange
    mock_post = mocker.patch("requests.post")
    mock_get = mocker.patch("requests.get")

    def mock_side_effect(url, TEST_RESPONSE):
        '''Функция, которая решает, какой ответ вернуть в зависимости от URL.'''
        if url in TEST_RESPONSE:
            mock_response = mocker.Mock()
            data = TEST_RESPONSE[url]
            if isinstance(data, str):  # это .text
                mock_response.text = data
            elif isinstance(data, dict):  # это .json()
                mock_response.text = json.dumps(data)
                mock_response.json = lambda: data
            return mock_response
        raise ValueError(f"Неожиданный URL: {url}")

    # Назначаем side_effect
    mock_post.side_effect = lambda url, *_args, **_kwargs: mock_side_effect(url, TEST_POST_RESPONSES)
    mock_get.side_effect = lambda url, *_args, **_kwargs: mock_side_effect(url, TEST_GET_RESPONSES)

    # Act
    RESULT = load_insert_data("01.01.2025", "03.01.2025")

    # Assert
    assert RESULT == EXPECTED_RESULT
    print("\n\nCALLS GET")
    print(mock_get.mock_calls)
    print("\n\nCALLS POST")
    print(mock_post.mock_calls)
