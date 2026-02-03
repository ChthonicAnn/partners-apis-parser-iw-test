# Рефакторинг парсера

Выполненное тестовое задание для собеседования на вакансию Junior Python Developer.
Моей задачей был рефакторинг исходного кода и перевод проекта на асинхронную архитектуру с добавлением автотестов и настройкой CI/CD.

## Технологический стек
![Python](https://img.shields.io/badge/python-3670A0?style=flat-square&logo=python&logoColor=ffdd54)
![Asyncio](https://img.shields.io/badge/Asyncio-%237D7D7D.svg?style=flat-square&logo=python&logoColor=white)
![HTTPX](https://img.shields.io/badge/HTTPX-%23000000.svg?style=flat-square&logo=python&logoColor=white)
![Pytest](https://img.shields.io/badge/Pytest-%230080FF.svg?style=flat-square&logo=pytest&logoColor=white)
![Unittest](https://img.shields.io/badge/Unittest-%237D7D7D.svg?style=flat-square&logo=python&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/github%20actions-%232671E5.svg?style=flat-square&logo=githubactions&logoColor=white)


### Что я сделала:

Изначальный проект представлял собой «божественную» функцию и не включал в себя архитектуру разделения на классы. Подробнее можно посмотреть по [ссылке](https://github.com/ChthonicAnn/partners-apis-parser-iw-test/blob/30c0d75b257dbe952975881f81ab52e9ac561422/legacy/parser.py).

Прежде чем рефакторить, написала один общий тест для «божественной» функции, чтобы точно не потерять никакую логику, и в дальнейшем валидировала на каждом этапе, что тест проходит.

Отрефакторила партнёров: теперь методы обработки вызываются по очереди, результаты работы предыдущего этапа являются входными данными для следующего. Затем я создала юнит-тесты для каждого партнёра, чтобы сохранить и их логику.

Заменила библиотеку request на асинхронную httpx и соответственно отредактировала под это тесты.

Сгруппировала партнёров по файлам SSP и DSP, как они были раньше в разных словарях. Новых партнёров, если их будут добавлять, предполагается выделять в отдельные файлы по каким-то ещё общим признакам. Это избавит нас от проблемы, которую вы описали в тестовом, что у нас один большой файл с кучей настроек партнёров.

### Что я могла бы сделать, но не было возможности:

В рамках тестового я не трогала ClickHouse, так как у меня нет к нему доступа, но по-хорошему, если рефакторить всё, то предполагается объединить SSP id и DSP id в один общий id и в ClickHouse сделать аналогичную миграцию.



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

