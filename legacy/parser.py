import asyncio
import datetime
import traceback
from multiprocessing.connection import Client

import pandas as pd
from dataclasses import asdict

from common.config import CLICKHOUSE_CONFIG_RO
from common.queue import queue
from legacy.abstract_partners import AbstractPartner
from legacy.partners.dsp_partners import DSPPartnerB, DSPPartnerF, DSPPartnerI, DSPPartnerM, DSPPartnerO
from legacy.partners.ssp_partners import SSPPartnerA, SSPPartnerB, SSPPartnerC, SSPPartnerD, SSPPartnerM, SSPPartnerO, SSPPartnerS
from .partner_data.data import PartnerData


def Partners_data_loader(start_date=None, finish_date=None):
    """Создаёт задачу в очереди для загрузки и вставки данных партнёров."""
    job = queue("load_insert_data", service="api").enqueue(load_insert_data, start_date, finish_date)
    return {'job_id': job.get_id()}, 200


def load_insert_data(start_date, finish_date):
    """Загружает данные всех партнёров за указанный период и вставляет их в базу."""
    start_date = pd.to_datetime(start_date) if start_date else datetime.date.today() - datetime.timedelta(days=1)
    finish_date = pd.to_datetime(finish_date) if finish_date else datetime.date.today() - datetime.timedelta(days=1)

    normal_partners = get_all_partners()
    data = asyncio.run(load(normal_partners, start_date, finish_date))
    insert(data)


def get_all_partners():
    """Возвращает список всех объектов партнёров (SSP и DSP)."""
    return [
        SSPPartnerM(),
        SSPPartnerB(),
        SSPPartnerO(),
        SSPPartnerS(),
        SSPPartnerC(),
        SSPPartnerD(),
        DSPPartnerI(),
        DSPPartnerO(),
        DSPPartnerM(),
        DSPPartnerB(),
        DSPPartnerF(),
        SSPPartnerA(),
    ]


async def load(normal_partners, start_date, finish_date):
    """Асинхронно загружает данные партнёров и агрегирует их."""
    tasks = [ok_parser(partner, start_date, finish_date) for partner in normal_partners]
    partners_data_list = await asyncio.gather(*tasks)

    data = []
    for partner_data_list in partners_data_list:
        data += agg_list_2keys_2values(partner_data_list)
    return data


async def ok_parser(normal_partner: AbstractPartner, start_date, finish_date):
    """Асинхронно получает данные одного партнёра за указанный период."""
    partner_data_list = []
    await normal_partner.authentificate()
    start_date_str = normal_partner.format_date(start_date)
    finish_date_str = normal_partner.format_date(finish_date)
    urls = normal_partner.get_urls(start_date_str, finish_date_str)

    await asyncio.gather(*(parse_one_url(normal_partner, url, partner_data_list) for url in urls))
    return partner_data_list


async def parse_one_url(normal_partner, url, partner_data_list):
    """Асинхронно обрабатывает один URL партнёра и добавляет результаты в список."""
    try:
        text = await normal_partner.request_data(url)
        dsp_id = 0
        ssp = ''
        if normal_partner.id.isdigit():
            dsp_id = int(normal_partner.id)
        else:
            ssp = normal_partner.id
        for record in normal_partner.norm_parse(text):
            partner_data = PartnerData(
                ssp=ssp,
                dsp_id=dsp_id,
                date=pd.to_datetime(record.date),
                imps=record.imps,
                spent=record.spent,
                currency=normal_partner.currency,
            )
            partner_data_list.append(partner_data)
    except Exception:
        traceback.print_exc()


def agg_list_2keys_2values(partner_data_list):  # [ssp/dsp,date,sum(imps),sum(spent)]
    """Агрегирует PartnerData по ключам (ssp/dsp_id + дата), суммируя показатели и затраты."""
    aggregated_data = {}

    for partner_data in partner_data_list:
        key = str(partner_data.ssp) + str(partner_data.dsp_id) + str(partner_data.date)
        if key in aggregated_data:
            aggregated_data[key].imps += partner_data.imps
            aggregated_data[key].spent += partner_data.spent
        else:
            aggregated_data[key] = partner_data

    return list(aggregated_data.values())


def insert(data):
    """Вставляет список PartnerData в таблицу ClickHouse."""
    query = "INSERT INTO dbname.partner_data (*) VALUES"
    client = Client(CLICKHOUSE_CONFIG_RO['host'])
    client.execute(query, (asdict(row) for row in data))
