import asyncio
import ssl
import pandas as pd
import os
import requests
from io import StringIO

final_date_format = '%Y-%m-%d %H:%M'
ssl._create_default_https_context = ssl._create_unverified_context

hourly_url = "https://www.cryptodatadownload.com/cdd/Coinbase_BTCUSD_1h.csv"
daily_url = "https://www.cryptodatadownload.com/cdd/Coinbase_BTCUSD_d.csv"


async def save_url_to_csv(url: str, date_format: str, file_name: str):
    # csv = pd.read_csv(url, encoding="iso-8859-1")  # header=1,
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0"}
    req = requests.get(url, headers=headers, verify=False)
    lines = req.text.splitlines()[1:]
    data = StringIO("\n".join(lines))
    csv = pd.read_csv(data)

    csv = csv.dropna(thresh=2)
    print(url)
    print(csv.columns)
    csv.columns = ['Date', 'Symbol', 'Open', 'High',
                   'Low', 'Close', 'Volume BTC', 'Volume USD']
    csv['Date'] = pd.to_datetime(csv['Date'], format=date_format)
    csv['Date'] = csv['Date'].dt.strftime(final_date_format)

    final_path = os.path.join('data', 'input', file_name)
    csv.to_csv(final_path, index=False)

    return csv


async def save_as_csv(hourly_url: str, daily_url: str):
    tasks = [save_url_to_csv(hourly_url, '%Y-%m-%d %I-%p', 'coinbase-1h-btc-usd.csv'),
             save_url_to_csv(daily_url, '%Y-%m-%d', 'coinbase-1d-btc-usd.csv')]
    # also FIRST_EXCEPTION and ALL_COMPLETED (default)
    done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
    print('>> done: ', done)
    # will be empty if using default return_when setting
    print('>> pending: ', pending)


def download_data_async():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(save_as_csv(hourly_url, daily_url))
    loop.close()


if __name__ == '__main__':
    download_data_async()
