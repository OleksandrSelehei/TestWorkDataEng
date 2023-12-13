import csv
import datetime
import requests
import json
import pyarrow.parquet as pq
import io
import time
from google.cloud import storage

headers = {"Authorization": "gAAAAABlcxyEvCSaRvqDrjz81ejMLcCBVC3GIb60pjTyFAfahhl_4WGM_RNAnDdq0yNYjSYfMbEVQaMzhNN8oCfNR2-b3VhhK75baPMcJ_n8NMwhzq_ulaGXkhhb4vrSUrXD30easSKRyxZGNmwex6a_o1ci85sLzJqsXwt-J7LuSgcy91tadt9H0zimiBu8VSDoENKCxdu98nihF86bIcqM9CehxD-lqt5t3skejwGZ--VUal1oZDt5pQoVGiJcaM8ccId8z9Vf"}
url_i = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/installs"
url_c = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/costs"
url_o = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/orders"
param_str_c = "channel,medium,campaign,keyword,ad_content,ad_group,landing_page,location"

current_date = datetime.datetime.now()
yesterday = current_date - datetime.timedelta(days=1)

formatted_yesterday = yesterday.strftime("%Y-%m-%d")
params = {"date": formatted_yesterday}
params_detail_c = {"date": formatted_yesterday, "dimensions": param_str_c}


def save_to_csv(filename, header, data):
    client = storage.Client(project="testwork-407514")
    bucket = client.get_bucket("ata-set-market")

    blob = bucket.blob(filename)

    csv_data = io.StringIO()
    csv_writer = csv.writer(csv_data)
    csv_writer.writerow(header)
    csv_writer.writerows(data)
    blob.upload_from_string(csv_data.getvalue(), content_type='text/csv')


def get_installs_data():
    while True:
        try:
            time.sleep(5)
            response_i = requests.get(url_i, headers=headers, params=params)
            response_i.raise_for_status()
            data = response_i.json()
            results = [(user["channel"], user["medium"], user["campaign"], user["keyword"], user["ad_content"],
                        user["ad_group"], user["landing_page"], user["alpha_2"], str(formatted_yesterday)) for user in json.loads(data["records"])]
            save_to_csv('installs.csv', ['channel', 'medium', 'campaign', 'keyword', 'ad_content', 'ad_group', 'landing_page', 'location', 'date'], results)
            break
        except:
            continue


def get_orders_data():
    while True:
        try:
            time.sleep(5)
            response_o = requests.get(url_o, headers=headers, params=params)
            if response_o.status_code == 200:
                parquet_stream = io.BytesIO(response_o.content)
                table = pq.read_table(parquet_stream)
                sum_ = 0
                for item in table["iap_item.price"]:
                    sum_ += float(item.as_py())
                save_to_csv('orders.csv', ['orders', 'date'], [(sum_, str(formatted_yesterday))])
                break
        except:
            continue


def get_costs_data():
    while True:
        try:
            time.sleep(5)
            response_c = requests.get(url_c, headers=headers, params=params)
            if response_c.status_code == 200:
                data = response_c.text
                save_to_csv("costs.csv", ["costs", 'date'], [(data.split("\n")[1], str(formatted_yesterday))])
                break
        except:
            continue


def get_detail_costs_data():
    while True:
        try:
            time.sleep(5)
            response_detail_c = requests.get(url_c, headers=headers, params=params_detail_c)
            if response_detail_c.status_code == 200:
                data = response_detail_c.text
                data_pre = data.split("\n")[1:-1]
                result = []
                for item in data_pre:
                    data_prom = item.split("\t")
                    data_prom.append(str(formatted_yesterday))
                    result.append(tuple(data_prom))
                save_to_csv("costs_detail.csv", ['channel', 'medium', 'ad_content', 'landing_page', 'ad_group', 'keyword', 'campaign', 'location', 'cost', 'date'], result)
                break
        except:
            continue


get_installs_data()
get_orders_data()
get_costs_data()
get_detail_costs_data()
