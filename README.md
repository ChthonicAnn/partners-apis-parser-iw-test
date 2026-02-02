## Архитектура парсера

**Структура:**
- `AbstractPartner` — базовый абстракный класс для всех партнёров.
- Каждая реализация (`SSPPartner*`, `DSPPartner*`) наследует его.
- Все они создают `PartnerRecord` из данных API.
- Эти `PartnerRecord` затем сводятся в общий список `PartnerData`, где лежат данные DSP и SSP партнеров.  

---

```mermaid
classDiagram
    class PartnerData {
        +date: date
        +dsp_id: int
        +ssp: str
        +imps: int
        +spent: float
        +currency: str
    }

    class PartnerRecord {
        +date: datetime
        +imps: int
        +spent: float
    }

    class AbstractPartner {
        <<abstract>>
        +urltype: str
        +id: str
        +currency: str
        +_headers: Dict[str, str]
        +authentificate() async
        +format_date(date: datetime) Any
        +get_urls(start_date, finish_date) List
        +request_data(url) async str
        +norm_parse(text: str) List[PartnerRecord]
    }

    %% ================== SSP PARTNERS (top row) ==================
    class SSPPartnerA
    class SSPPartnerB
    class SSPPartnerC
    class SSPPartnerD
    class SSPPartnerM
    class SSPPartnerO
    class SSPPartnerS

    %% ================== DSP PARTNERS (bottom row) ==================
    class DSPPartnerB
    class DSPPartnerF
    class DSPPartnerI
    class DSPPartnerM
    class DSPPartnerO

    %% ================== INHERITANCE ==================
    AbstractPartner <|-- SSPPartnerA
    AbstractPartner <|-- SSPPartnerB
    AbstractPartner <|-- SSPPartnerC
    AbstractPartner <|-- SSPPartnerD
    AbstractPartner <|-- SSPPartnerM
    AbstractPartner <|-- SSPPartnerO
    AbstractPartner <|-- SSPPartnerS

    AbstractPartner <|-- DSPPartnerB
    AbstractPartner <|-- DSPPartnerF
    AbstractPartner <|-- DSPPartnerI
    AbstractPartner <|-- DSPPartnerM
    AbstractPartner <|-- DSPPartnerO

    %% ================== RELATIONSHIPS ==================
    PartnerRecord --> PartnerData : aggregated into
    AbstractPartner --> PartnerRecord : produces
