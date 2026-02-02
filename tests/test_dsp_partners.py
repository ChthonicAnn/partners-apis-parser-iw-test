import asyncio
from datetime import datetime
import json
from unittest.mock import AsyncMock, MagicMock
import pytest

from legacy.abstract_partners import PartnerRecord
from legacy.partners.dsp_partners import DSPPartnerB, DSPPartnerF, DSPPartnerI, DSPPartnerM, DSPPartnerO


class TestDSPPartnerI:
    @pytest.fixture
    def partner(self):
        return DSPPartnerI()

    def test_valid_json_returns_multiple_partner_records(self, partner):
        """Проверяет корректное преобразование нескольких элементов JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        multi_json = {
            "data": [
                {"date": "2025-01-01", "imp": 123, "revenue": 456.7},
                {"date": "2025-01-02", "imp": 10, "revenue": 12.3},
            ]
        }
        text = json.dumps(multi_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 2
        assert all(isinstance(r, PartnerRecord) for r in result)

        assert result[0].date == "2025-01-01"
        assert result[0].imps == 123
        assert result[0].spent == 456.7

        assert result[1].date == "2025-01-02"
        assert result[1].imps == 10
        assert result[1].spent == 12.3

    def test_valid_json_returns_single_partner_record(self, partner):
        """Проверяет корректное преобразование одного элемента JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        single_json = {
            "data": [
                {"date": "2025-01-01", "imp": 123, "revenue": 456.7}
            ]
        }
        text = json.dumps(single_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 1
        assert isinstance(result[0], PartnerRecord)
        assert result[0].date == "2025-01-01"
        assert result[0].imps == 123
        assert result[0].spent == 456.7

    def test_empty_data_returns_empty_list(self, partner):
        """Проверяет, что при пустом списке 'data' возвращается пустой результат."""
        # ----------------- Arrange -----------------
        test_json = {"data": []}
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert result == []

    def test_null_values_raise_exception(self, partner):
        """Проверяет, что если внутри JSON null, метод norm_parse бросает TypeError."""
        # ----------------- Arrange -----------------
        test_json = {
            "data": [
                {"date": "2025-01-01", "imp": None, "revenue": None}
            ]
        }
        text = json.dumps(test_json)

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(text))

    def test_format_date_returns_correct_string(self, partner):
        """Проверяет, что метод format_date возвращает строку даты в формате 'YYYYMMDD'."""
        # ----------------- Arrange -----------------
        test_date = datetime(2025, 10, 30)
        expected_str = "20251030"

        # ----------------- Act -----------------
        result = partner.format_date(test_date)

        # ----------------- Assert -----------------
        assert result == expected_str


class TestDSPPartnerO:
    @pytest.fixture
    def partner(self):
        return DSPPartnerO()

    def test_valid_json_multiple_elements(self, partner):
        """Проверяет корректное преобразование нескольких объектов JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        multi_json = {
            "data": [
                {"day": "01-01-2025", "impressions": 123, "earnings": 4567},
                {"day": "02-01-2025", "impressions": 10, "earnings": 1230},
            ]
        }
        text = json.dumps(multi_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 2
        assert result[0].date == "2025-01-01"
        assert result[0].imps == 123
        assert result[0].spent == 4.567

        assert result[1].date == "2025-01-02"
        assert result[1].imps == 10
        assert result[1].spent == 1.23

    def test_valid_json_single_element(self, partner):
        """Проверяет корректное преобразование одного объекта JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        single_json = {
            "data": [
                {"day": "01-01-2025", "impressions": 123, "earnings": 4567}
            ]
        }
        text = json.dumps(single_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 1
        assert isinstance(result[0], PartnerRecord)
        assert result[0].date == "2025-01-01"
        assert result[0].imps == 123
        assert result[0].spent == 4.567

    def test_empty_data_returns_empty_list(self, partner):
        """Проверяет, что при пустом списке 'data' возвращается пустой результат."""
        # ----------------- Arrange -----------------
        test_json = {"data": []}
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert result == []

    def test_null_values_raise_exception(self, partner):
        """Проверяет, что если внутри JSON null, метод norm_parse бросает TypeError."""
        # ----------------- Arrange -----------------
        test_json = {
            "data": [
                {"day": "01-01-2025", "impressions": None, "earnings": None}
            ]
        }
        text = json.dumps(test_json)

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(text))


class TestDSPPartnerM:
    @pytest.fixture
    def partner(self):
        return DSPPartnerM()

    def test_valid_json_returns_multiple_partner_records(self, partner):
        """Проверяет корректное преобразование нескольких элементов JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_json = {
            "items": [
                {
                    "rows": [
                        {"date": "2025-01-01", "shows": 123, "amount": 456.7},
                        {"date": "2025-01-02", "shows": 10.0, "amount": 12.3},
                    ]
                },
                {
                    "rows": [
                        {"date": "2025-01-03", "shows": 5, "amount": 7.7}
                    ]
                }
            ]
        }
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 3
        assert isinstance(result[0], PartnerRecord)
        assert result[0].date == "2025-01-01"
        assert result[0].imps == 123
        assert result[0].spent == 456.7

        assert result[1].date == "2025-01-02"
        assert result[1].imps == 10
        assert result[1].spent == 12.3

        assert result[2].date == "2025-01-03"
        assert result[2].imps == 5
        assert result[2].spent == 7.7

    def test_valid_json_returns_single_partner_record(self, partner):
        """Проверяет корректное преобразование одного элемента JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_json = {
            "items": [
                {
                    "rows": [
                        {"date": "2025-01-01", "shows": 123, "amount": 456.7}
                    ]
                }
            ]
        }
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 1
        assert isinstance(result[0], PartnerRecord)
        assert result[0].date == "2025-01-01"
        assert result[0].imps == 123
        assert result[0].spent == 456.7

    def test_empty_rows_returns_empty_list(self, partner):
        """Проверяет, что при пустом списке rows возвращается пустой результат."""
        # ----------------- Arrange -----------------
        test_json = {"items": [{"rows": []}]}
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert result == []

    def test_null_values_raise_exception(self, partner):
        """Проверяет, что если внутри JSON null, метод norm_parse бросает TypeError."""
        # ----------------- Arrange -----------------
        test_json = {
            "items": [
                {
                    "rows": [
                        {"date": "2025-01-01", "shows": None, "amount": None}
                    ]
                }
            ]
        }
        text = json.dumps(test_json)

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(text))


class TestDSPPartnerB:
    @pytest.fixture
    def partner(self):
        return DSPPartnerB()

    def test_valid_json_returns_multiple_partner_records(self, partner):
        """Проверяет корректное преобразование нескольких дат JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_json = {
            "data": {
                "total": {
                    "date": ["2025-01-01", "2025-01-02", "2025-01-03"],
                    "count_imps": {
                        "2025-01-01": 100,
                        "2025-01-02": 200,
                        "2025-01-03": 50
                    },
                    "total_pub_payable": {
                        "2025-01-01": 10.5,
                        "2025-01-02": 20.0,
                        "2025-01-03": 5.0
                    }
                }
            }
        }
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 3
        assert isinstance(result[0], PartnerRecord)
        assert result[0].date == "2025-01-01"
        assert result[0].imps == 100
        assert result[0].spent == 10.5

        assert result[1].date == "2025-01-02"
        assert result[1].imps == 200
        assert result[1].spent == 20.0

        assert result[2].date == "2025-01-03"
        assert result[2].imps == 50
        assert result[2].spent == 5.0

    def test_valid_json_returns_single_partner_record(self, partner):
        """Проверяет корректное преобразование одной даты JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_json = {
            "data": {
                "total": {
                    "date": ["2025-01-01"],
                    "count_imps": {"2025-01-01": 123},
                    "total_pub_payable": {"2025-01-01": 45.6}
                }
            }
        }
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 1
        assert isinstance(result[0], PartnerRecord)
        assert result[0].date == "2025-01-01"
        assert result[0].imps == 123
        assert result[0].spent == 45.6

    def test_empty_total_returns_empty_list(self, partner):
        """Проверяет, что при пустом total возвращается пустой результат."""
        # ----------------- Arrange -----------------
        test_json = {"data": {"total": {"date": [], "count_imps": {}, "total_pub_payable": {}}}}
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert result == []

    def test_null_values_raise_exception(self, partner):
        """Проверяет, что если внутри JSON null, метод norm_parse бросает TypeError."""
        # ----------------- Arrange -----------------
        test_json = {
            "data": {
                "total": {
                    "date": ["2025-01-01"],
                    "count_imps": {"2025-01-01": None},
                    "total_pub_payable": {"2025-01-01": None}
                }
            }
        }
        text = json.dumps(test_json)

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(text))

    def test_authentificate_sets_token_and_request_data_uses_it(self, partner, mocker):
        """Тестирует, что метод authentificate устанавливает токен, а request_data использует его в заголовках."""
        # ----------------- Arrange -----------------
        fake_token = "fake-dsp-token-789"
        fake_url = "https://example.com/data"
        fake_response_text = "response content"

        mock_client = mocker.patch("httpx.AsyncClient", autospec=True)
        mock_instance = mock_client.return_value.__aenter__.return_value

        mock_post = AsyncMock()
        mock_get = AsyncMock()

        mock_post.return_value = MagicMock(json=lambda: {"data": fake_token})
        mock_get.return_value = MagicMock(text=fake_response_text)

        mock_instance.post = mock_post
        mock_instance.get = mock_get

        # ----------------- Act -----------------
        asyncio.run(partner.authentificate())
        result = asyncio.run(partner.request_data(fake_url))

        # ----------------- Assert -----------------
        mock_instance.get.assert_called_once()
        called_args, called_kwargs = mock_instance.get.call_args
        assert called_args[0] == fake_url
        assert called_kwargs["headers"]["Authorization"] == f"Token {fake_token}"
        assert result == fake_response_text


class TestDSPPartnerF:
    @pytest.fixture
    def partner(self):
        return DSPPartnerF()

    def test_valid_xml_returns_partner_records(self, partner):
        """Проверяет корректное преобразование валидного XML в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_xml = """
        <root>
            <row>
                <date>2025-01-01</date>
                <impressions>123</impressions>
                <revenue>456.7</revenue>
            </row>
            <row>
                <date>2025-01-02</date>
                <impressions>10</impressions>
                <revenue>12.3</revenue>
            </row>
        </root>
        """

        # ----------------- Act -----------------
        result = list(partner.norm_parse(test_xml))

        # ----------------- Assert -----------------
        assert all(isinstance(r, PartnerRecord) for r in result)
        assert len(result) == 2

        assert result[0].date == "2025-01-01"
        assert result[0].imps == 123
        assert result[0].spent == 456.7

        assert result[1].date == "2025-01-02"
        assert result[1].imps == 10
        assert result[1].spent == 12.3

    def test_empty_xml_returns_empty_list(self, partner):
        """Проверяет, что при пустом XML возвращается пустой результат."""
        # ----------------- Arrange -----------------
        test_xml = "<root></root>"

        # ----------------- Act -----------------
        result = list(partner.norm_parse(test_xml))

        # ----------------- Assert -----------------
        assert result == []

    def test_invalid_values_raise_exception(self, partner):
        """Проверяет, что некорректные или отсутствующие значения вызывают исключение."""
        # ----------------- Arrange -----------------
        test_xml = """
        <root>
            <row>
                <date>2025-01-01</date>
                <impressions></impressions>
                <revenue></revenue>
            </row>
        </root>
        """

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(test_xml))
