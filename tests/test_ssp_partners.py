import asyncio
from datetime import datetime
import json
from unittest.mock import AsyncMock, MagicMock
import pytest

from legacy.abstract_partners import PartnerRecord
from legacy.partners.ssp_partners import SSPPartnerA, SSPPartnerB, SSPPartnerC, SSPPartnerD, SSPPartnerM, SSPPartnerO, SSPPartnerS


class TestSSPPartnerM:
    @pytest.fixture
    def partner(self):
        return SSPPartnerM()

    def test_valid_json_returns_partner_records(self, partner):
        """Проверяет корректное преобразование валидного JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_json = {
            "items": [
                {
                    "rows": [
                        {"date": "2025-01-01", "base": {"shows": 123, "spent": 456.7}},
                        {"date": "2025-01-02", "base": {"shows": 10.0, "spent": 12.3}},
                    ]
                },
                {
                    "rows": [
                        {"date": "2025-01-03", "base": {"shows": 5, "spent": 7.7}}
                    ]
                }
            ]
        }
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert all(isinstance(r, PartnerRecord) for r in result)
        assert len(result) == 2

        assert result[0].date == "2025-01-01"
        assert result[0].imps == 123
        assert result[0].spent == 456.7
        assert result[1].date == "2025-01-02"
        assert result[1].imps == 10
        assert result[1].spent == 12.3

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
                        {"date": "2025-01-01", "base": {"shows": None, "spent": None}}
                    ]
                }
            ]
        }
        text = json.dumps(test_json)

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(text))


class TestSSPPartnerB:
    @pytest.fixture
    def partner(self):
        return SSPPartnerB()

    def test_valid_json_returns_multiple_partner_records(self, partner):
        """Проверяет корректное преобразование нескольких элементов JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_json = {
            "data": {
                "total": {
                    "date": ["2025-01-01", "2025-01-02"],
                    "count_imps": {"2025-01-01": 123, "2025-01-02": 456},
                    "net_payable_data": {"2025-01-01": 456.7, "2025-01-02": 78.9},
                }
            }
        }
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert all(isinstance(r, PartnerRecord) for r in result)
        assert len(result) == 2

        assert result[0].date == "2025-01-01"
        assert result[0].imps == 123
        assert result[0].spent == 456.7

        assert result[1].date == "2025-01-02"
        assert result[1].imps == 456
        assert result[1].spent == 78.9

    def test_valid_json_returns_single_partner_record(self, partner):
        """Проверяет корректное преобразование одного элемента JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_json = {
            "data": {
                "total": {
                    "date": ["2025-01-03"],
                    "count_imps": {"2025-01-03": 10},
                    "net_payable_data": {"2025-01-03": 1.23},
                }
            }
        }
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 1
        assert isinstance(result[0], PartnerRecord)
        assert result[0].date == "2025-01-03"
        assert result[0].imps == 10
        assert result[0].spent == 1.23

    def test_empty_total_returns_empty_list(self, partner):
        """Проверяет, что при пустом total возвращается пустой результат."""
        # ----------------- Arrange -----------------
        test_json = {"data": {"total": {"date": [], "count_imps": {}, "net_payable_data": {}}}}
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
                    "net_payable_data": {"2025-01-01": None},
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
        fake_token = "fake-token-123"
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


class TestSSPPartnerO:
    @pytest.fixture
    def partner(self):
        return SSPPartnerO()

    def test_valid_json_returns_multiple_partner_records(self, partner):
        """Проверяет корректное преобразование нескольких элементов JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_json = {
            "data": [
                {"date": "2025-01-01", "impressionCount": 123, "spent": 456.7},
                {"date": "2025-01-02", "impressionCount": 10, "spent": 12.3},
            ]
        }
        text = json.dumps(test_json)

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
        test_json = {
            "data": [
                {"date": "2025-01-03", "impressionCount": 50, "spent": 7.5}
            ]
        }
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 1
        assert isinstance(result[0], PartnerRecord)
        assert result[0].date == "2025-01-03"
        assert result[0].imps == 50
        assert result[0].spent == 7.5

    def test_empty_data_returns_empty_list(self, partner):
        """Проверяет, что при пустом data возвращается пустой результат."""
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
                {"date": "2025-01-01", "impressionCount": None, "spent": None}
            ]
        }
        text = json.dumps(test_json)

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(text))


class TestSSPPartnerS:
    @pytest.fixture
    def partner(self):
        return SSPPartnerS()

    def test_valid_json_returns_multiple_partner_records(self, partner):
        """Проверяет корректное преобразование нескольких элементов JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_json = {
            "2025-01-01": {"impressions": 100, "revenue": 10.5},
            "2025-01-02": {"impressions": 200, "revenue": 20.0},
        }
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert all(isinstance(r, PartnerRecord) for r in result)
        assert len(result) == 2

        assert result[0].date == "2025-01-01"
        assert result[0].imps == 100
        assert result[0].spent == 10.5

        assert result[1].date == "2025-01-02"
        assert result[1].imps == 200
        assert result[1].spent == 20.0

    def test_valid_json_returns_single_partner_record(self, partner):
        """Проверяет корректное преобразование одного элемента JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        test_json = {
            "2025-01-01": {"impressions": 100, "revenue": 10.5}
        }
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 1
        assert isinstance(result[0], PartnerRecord)
        assert result[0].date == "2025-01-01"
        assert result[0].imps == 100
        assert result[0].spent == 10.5

    def test_empty_items_returns_empty_list(self, partner):
        """Проверяет, что при пустом JSON возвращается пустой результат."""
        # ----------------- Arrange -----------------
        test_json = {}
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert result == []

    def test_null_values_raise_exception(self, partner):
        """Проверяет, что если внутри JSON null, метод norm_parse бросает TypeError."""
        # ----------------- Arrange -----------------
        test_json = {
            "2025-01-01": {"impressions": None, "revenue": None}
        }
        text = json.dumps(test_json)

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(text))


class TestSSPPartnerC:
    @pytest.fixture
    def partner(self):
        return SSPPartnerC()

    def test_valid_xml_returns_partner_records(self, partner):
        """Проверяет корректное преобразование валидного XML в PartnerRecord."""
        # ----------------- Arrange -----------------
        xml_text = """
        <root>
            <row date="2025-01-01">
                <impressions>100</impressions>
                <revenue>10.5</revenue>
            </row>
            <row date="2025-01-02">
                <impressions>200</impressions>
                <revenue>20.0</revenue>
            </row>
        </root>
        """

        # ----------------- Act -----------------
        result = list(partner.norm_parse(xml_text))

        # ----------------- Assert -----------------
        assert all(isinstance(r, PartnerRecord) for r in result)
        assert len(result) == 2

        assert result[0].date == "2025-01-01"
        assert result[0].imps == 100
        assert result[0].spent == 10.5

        assert result[1].date == "2025-01-02"
        assert result[1].imps == 200
        assert result[1].spent == 20.0

    def test_empty_xml_returns_empty_list(self, partner):
        """Проверяет, что пустой XML возвращает пустой результат."""
        # ----------------- Arrange -----------------
        xml_text = "<root></root>"

        # ----------------- Act -----------------
        result = list(partner.norm_parse(xml_text))

        # ----------------- Assert -----------------
        assert result == []

    def test_missing_values_raise_exception(self, partner):
        """Проверяет, что если внутри XML нет текста, метод norm_parse бросает TypeError."""
        # ----------------- Arrange -----------------
        xml_text = """
        <root>
            <row date="2025-01-01">
                <impressions></impressions>
                <revenue></revenue>
            </row>
        </root>
        """

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(xml_text))


class TestSSPPartnerD:
    @pytest.fixture
    def partner(self):
        return SSPPartnerD()

    def test_valid_xml_returns_partner_records(self, partner):
        """Проверяет корректное преобразование валидного XML в PartnerRecord."""
        # ----------------- Arrange -----------------
        xml_text = """
        <root>
            <row date="2025-01-01">
                <impressions>6000</impressions>
                <revenue>54.0</revenue>
            </row>
            <row date="2025-01-02">
                <impressions>6900</impressions>
                <revenue>60.0</revenue>
            </row>
        </root>
        """

        # ----------------- Act -----------------
        result = list(partner.norm_parse(xml_text))

        # ----------------- Assert -----------------
        assert all(isinstance(r, PartnerRecord) for r in result)
        assert len(result) == 2

        assert result[0].date == "2025-01-01"
        assert result[0].imps == 6000
        assert result[0].spent == 54.0

        assert result[1].date == "2025-01-02"
        assert result[1].imps == 6900
        assert result[1].spent == 60.0

    def test_empty_xml_returns_empty_list(self, partner):
        """Проверяет, что пустой XML возвращает пустой результат."""
        # ----------------- Arrange -----------------
        xml_text = "<root></root>"

        # ----------------- Act -----------------
        result = list(partner.norm_parse(xml_text))

        # ----------------- Assert -----------------
        assert result == []

    def test_missing_values_raise_exception(self, partner):
        """Проверяет, что если внутри XML нет текста, метод norm_parse бросает TypeError."""
        # ----------------- Arrange -----------------
        xml_text = """
        <root>
            <row date="2025-01-01">
                <impressions></impressions>
                <revenue></revenue>
            </row>
        </root>
        """

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(xml_text))


class TestSSPPartnerA:
    @pytest.fixture
    def partner(self):
        return SSPPartnerA()

    def test_valid_json_returns_multiple_partner_records(self, partner):
        """Проверяет корректное преобразование нескольких элементов JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        multi_json = {
            "data": {
                "20250820": {"impression_count": 1234, "cost": 12.5},
                "20250821": {"impression_count": 5678, "cost": 45.6},
            }
        }
        text = json.dumps(multi_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert all(isinstance(r, PartnerRecord) for r in result)
        assert len(result) == 2

        assert result[0].date == "20250820"
        assert result[0].imps == 1234
        assert result[0].spent == 12.5

        assert result[1].date == "20250821"
        assert result[1].imps == 5678
        assert result[1].spent == 45.6

    def test_valid_json_returns_single_partner_record(self, partner):
        """Проверяет корректное преобразование одного элемента JSON в PartnerRecord."""
        # ----------------- Arrange -----------------
        single_json = {
            "data": {
                "20250820": {"impression_count": 1234, "cost": 12.5},
            }
        }
        text = json.dumps(single_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert len(result) == 1
        assert isinstance(result[0], PartnerRecord)
        assert result[0].date == "20250820"
        assert result[0].imps == 1234
        assert result[0].spent == 12.5

    def test_empty_data_returns_empty_list(self, partner):
        """Проверяет, что при пустом data возвращается пустой результат."""
        # ----------------- Arrange -----------------
        test_json = {"data": {}}
        text = json.dumps(test_json)

        # ----------------- Act -----------------
        result = list(partner.norm_parse(text))

        # ----------------- Assert -----------------
        assert result == []

    def test_null_values_raise_exception(self, partner):
        """Проверяет, что если внутри JSON None, метод norm_parse бросает TypeError."""
        # ----------------- Arrange -----------------
        test_json = {
            "data": {
                "20250822": {"impression_count": None, "cost": None},
                "20250823": {}
            }
        }
        text = json.dumps(test_json)

        # ----------------- Act & Assert -----------------
        with pytest.raises(TypeError):
            list(partner.norm_parse(text))

    def test_authentificate_sets_token_and_request_data_uses_it(self, mocker, partner):
        """Тестирует, что метод authentificate устанавливает токен, а request_data использует его в заголовках."""
        # ----------------- Arrange -----------------
        fake_token = "fake-token-456"
        fake_url = "https://example.com/data"
        fake_response_text = "response content"

        mock_client = mocker.patch("httpx.AsyncClient", autospec=True)
        mock_instance = mock_client.return_value.__aenter__.return_value

        mock_post = AsyncMock()
        mock_get = AsyncMock()

        mock_post.return_value = MagicMock(json=lambda: {"access_token": fake_token})
        mock_get.return_value = MagicMock(text=fake_response_text)

        mock_instance.post = mock_post
        mock_instance.get = mock_get

        # ----------------- Act -----------------
        asyncio.run(partner.authentificate())
        result = asyncio.run(partner.request_data(fake_url))

        # ----------------- Assert -----------------
        mock_get.assert_called_once()
        called_args, called_kwargs = mock_get.call_args
        assert called_args[0] == fake_url
        assert called_kwargs["headers"]["Authorization"] == f"Bearer {fake_token}"
        assert result == fake_response_text

    def test_format_date_returns_correct_string(self, partner):
        """Проверяет, что метод format_date возвращает строку даты в формате 'YYYYMMDD'."""
        # ----------------- Arrange -----------------
        test_date = datetime(2025, 10, 30)
        expected_str = "20251030"

        # ----------------- Act -----------------
        result = partner.format_date(test_date)

        # ----------------- Assert -----------------
        assert result == expected_str
