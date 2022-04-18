#!/usr/bin/env python3
import os
import requests
import time
import logging as l
from termcolor import colored
from datetime import datetime as dt
from datetime import timedelta
from bs4 import BeautifulSoup
from sheetfu import SpreadsheetApp, Table
from dataclasses import dataclass
from schema import Dump, get_db


SHEET_NAME = ''
TOKEN_PATH = './secrets/sa.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15'
    }
PULL_INTERVAL = 3600 * 24
THROTTLE_TIME = 1


def get_apartments_from_google_sheets():
  l.info('Getting data from spreadsheet...')
  sa = SpreadsheetApp(TOKEN_PATH)
  s = sa.open_by_id(SHEET_NAME).sheets[0]
  table = Table(s.get_data_range())

  return [
      {k.lower(): v for k, v in a.to_dict().items()}
      for a in table]


def main():
  conn, c = get_db()
  
  # get latest dumps for each url
  res = c.execute('''
  SELECT t1.*
  FROM dumps t1
  JOIN (
    SELECT url, max(timestamp) as timestamp
    FROM dumps t2
    WHERE status = 200
    GROUP BY url
  ) t2 on t1.timestamp = t2.timestamp
  ''')
  dumps = [Dump(*r) for r in res]
  dumps_by_url = {d.url: d for d in dumps}

  # get apartments from google sheets
  apmts = get_apartments_from_google_sheets()
  for a in apmts:
    # get dump if outdated
    cur_ts = dt.now().timestamp()
    delta = (timedelta(seconds=int(cur_ts - dumps_by_url[a['url']].timestamp))
        if a['url'] in dumps_by_url else 0)

    if not delta or delta > PULL_INTERVAL:
      l.info(f"Pulling data for: {colored(a['name'], 'green')} after {str(delta)}")
      r = requests.get(a['url'], headers=HEADERS)

      d = Dump(0, a['url'], dt.now().timestamp(), r.status_code, r.text)
      d.insert(conn, c)
      dumps_by_url[d.url] = d
      time.sleep(THROTTLE_TIME)

    else:
      l.info(f"Skipping {colored(a['name'], 'green')}: Last pulled {str(delta)} ago")




if __name__ == "__main__":
  l.basicConfig(level=l.INFO)
  # l.basicConfig(level=l.DEBUG)
  main()
