from multiprocessing.connection import Client
import traceback

from common.config import CLICKHOUSE_CONFIG_RO
from legacy.abstract_partners import AbstractPartner
from legacy.partners.dsp_partners import DSPPartnerB, DSPPartnerF, DSPPartnerI, DSPPartnerM, DSPPartnerO
from legacy.partners.ssp_partners import SSPPartnerA, SSPPartnerB, SSPPartnerC, SSPPartnerD, SSPPartnerM, SSPPartnerO, SSPPartnerS
# from common.queue import queue
# from .import remote_file
import datetime
import xml.etree.cElementTree as ET
# import numpy as np
# import time
# from minio import Minio
# from io import BytesIO
# import uuid
# import re

# from collections import defaultdict
# from functools import reduce

# from itertools import groupby
# from functools import partial
# from .remote_file import load_pickle_from_minio

import pandas as pd
# import json
# from clickhouse_driver import Client
from dataclasses import asdict
# from .utils import read_clickhouse

import xmlrpc.client
# import ssl
# from itertools import chain

from .partner_data.data import PartnerData2, PartnerData
# from .partner_data.partners import insert_recent_partner_s_data


class CookiesTransport(xmlrpc.client.SafeTransport):
    """A SafeTransport (HTTPS) subclass that retains cookies over its lifetime."""

    def __init__(self, context=None):
        super().__init__(context=context)
        self._cookies = []

    def send_headers(self, connection, headers):
        if self._cookies:
            connection.putheader("Cookie", "; ".join(self._cookies))
        super().send_headers(connection, headers)

    def parse_response(self, response):
        if response.msg.get_all("Set-Cookie"):
            for header in response.msg.get_all("Set-Cookie"):
                cookie = header.split(";", 1)[0]
                self._cookies.append(cookie)
        return super().parse_response(response)


def ok_parser(normal_partner: AbstractPartner, start_date, finish_date):
    print('ok_parser start')
    print(start_date, finish_date)
    dd = []
    normal_partner.authentificate()
    start_date_str = normal_partner.format_date(start_date)
    finish_date_str = normal_partner.format_date(finish_date)
    urls = normal_partner.get_urls(start_date_str, finish_date_str)
    for url in urls:
        try:
            text = normal_partner.request_data(url)
            print(text)
            # print(res.text)
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
                dd.append(partner_data)

        except Exception as e:
            traceback.print_exc()
            print()
            print()
            print(e)
            print('Exception')
        print('ok_parser finish')
        print(dd)
    return dd


def agg_list_2keys_2values(dd):  # [ssp/dsp,date,sum(imp),sum(money)]
    q = {}
    for x in dd:
        key = str(x.ssp) + str(x.dsp_id) + str(x.date)
        if key in q:
            q[key].imps += x.imps
            q[key].spent += x.spent
        else:
            q[key] = x
    return list(q.values())


def load_insert_data(start_date, finish_date):
    start_date = pd.to_datetime(start_date) if start_date else datetime.date.today() - datetime.timedelta(days=1)
    finish_date = pd.to_datetime(finish_date) if finish_date else datetime.date.today() - datetime.timedelta(days=1)
    data = []

    NORMAL_PARTNERS = [
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


    for normal_partner in NORMAL_PARTNERS:
        data += agg_list_2keys_2values(ok_parser(normal_partner, start_date, finish_date))

    # # __________________ SSP _____________________________________________________
    # for ssp in API_PARTNERS_SSP:
    #     data += agg_list_2keys_2values(process_ssp(ssp))

    # # __________________________________ DSP _________________________________________________________
    # for dsp_id in API_PARTNERS_DSP:
    #     data += agg_list_2keys_2values(process_dsp(dsp_id))

    # ### Processing S - CUSTOM
    # print(f"loading partner S")
    # transport = CookiesTransport(context=ssl._create_unverified_context())
    # result = None
    # for ep in [178, 209, 211, 213]:
    #     try:
    #         with xmlrpc.client.ServerProxy("https://dsp-partner-s.example/xmlrpc/", transport) as proxy:
    #             proxy.partner_s.login(PARTNER_S_DSP_LOGIN, PARTNER_S_DSP_TOKEN)

    #         with xmlrpc.client.ServerProxy("https://dsp-partner-s.example/xmlrpc/", transport) as proxy:
    #             res = pd.DataFrame(proxy.rtb.get_openrtb_stats((start_date).strftime("%Y-%m-%d %H:%M:%S"),
    #                                                             finish_date.strftime("%Y-%m-%d %H:%M:%S"),
    #                                                             1,
    #                                                             ep))
    #             res['date'] = res['date_view'].apply(lambda x: pd.to_datetime(str(x), format = '%Y%m%dT%H:%M:%S').date())
    #             result = pd.concat((result, res))
    #         print(f"ep={ep} {result}")
    #     except:
    #         print(f"bad ep={ep}")
    # result = result.groupby('date', as_index = False).agg({'imps':'sum', 'amount':'sum'})
    # print("result=")
    # print(result)
    # data+=[PartnerData(date=pd.to_datetime(row['date']),
    #                           dsp_id=58,
    #                           imps=int(row['imps']),
    #                           spent=float(row['amount']),
    #                           currency='rub') for _,row in result.iterrows()]

    # ### Processing partner G : 123 - CUSTOM
    # # G : 123
    # tmp_date=start_date
    # dsp_id=123
    # dd=[]
    # fun=lambda _data: map(lambda x: x.split('\t'), filter(lambda d: re.match(r'^[0-9]', d),_data.split('\n')))

    # while tmp_date<=finish_date:
    #     try:
    #         url=f'https://dsp-partner-g.example/api/v2/reports?start={tmp_date.strftime("%Y-%m-%d")}&end={tmp_date.strftime("%Y-%m-%d")}'
    #         print(dsp_id,tmp_date, url)
    #         res = requests.get(url)
    #         print(res)
    #         print(res.text)
    #         _data = res.text
    #         dd = dd + list(map(lambda d: PartnerData(dsp_id=dsp_id, date=pd.to_datetime(tmp_date), imps=int(float(d[0])), spent=float(d[1]),currency='rub'),
    #                            list(fun(_data))))
    #         tmp_date+=datetime.timedelta(days=1)
    #     except Exception as e:
    #         print(e)
    # data += agg_list_2keys_2values(dd)

    # ### Processing partner B dsp - CUSTOM
    # print("Processing partner B dsp")
    # curr_date = start_date
    # b_dsp_id = 376
    # b_dsp_data = []

    # fun = lambda _json: _json['statistic']
    # while curr_date <= finish_date:
    #     try:
    #         url = 'https://dsp-partner-b.example/stats'
    #         payload = {
    #             "start_date": curr_date.strftime("%d-%m-%Y"),
    #             "end_date": curr_date.strftime("%d-%m-%Y"),
    #             "field_names": ["impressions", "clicks", "revenue"],
    #             "group_by": ["site_id", "placement_id"]
    #         }
    #         res = requests.post(url, json=payload, headers={'Authorization': f'Bearer {DSP_B_ACCESS_TOKEN}'})
    #         _data = res.json()
    #         total_impressions = 0
    #         total_revenue = 0.0

    #         # Aggregating by data from list in one curr_date
    #         for item in fun(_data):
    #             total_impressions += int(item['impressions'])
    #             total_revenue += float(item['revenue'])

    #         # Create one object PartnerData for curr_date
    #         b_dsp_data.append(PartnerData(
    #             dsp_id=b_dsp_id,
    #             date=pd.to_datetime(curr_date),
    #             imps=total_impressions,
    #             spent=total_revenue,
    #             # currency='rub'
    #         ))

    #         curr_date += datetime.timedelta(days=1)
    #     except Exception as e:
    #         print("b dsp error", e)

    # data += agg_list_2keys_2values(b_dsp_data)

    # print("FINAL PartnerData for partners :", data,)
    # for pr in data:
    #     print(pr.dsp_id, pr.ssp, pr)

    # return data

    query = "INSERT INTO dbname.partner_data (*) VALUES"
    client = Client(CLICKHOUSE_CONFIG_RO['host'])
    client.execute(query, (asdict(row) for row in data))


def Partners_data_loader(start_date=None, finish_date=None):
    job = queue("load_insert_data", service="api").enqueue(load_insert_data, start_date, finish_date)
    return {'job_id': job.get_id()}, 200
