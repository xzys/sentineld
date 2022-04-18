#!/usr/bin/env python3
import os
import requests
import time
import json
import argparse
import logging as l
from termcolor import colored
from datetime import datetime as dt
from datetime import timedelta
from bs4 import BeautifulSoup
from sheetfu import SpreadsheetApp, Table
from dataclasses import dataclass
from schema import Dump, get_db


TOKEN_PATH = './secrets/sa.json'
with open('./secrets/sheet_name') as f:
  SHEET_NAME = f.read().strip()
LOCAL_SHEETS_DATA_FN = 'sheets_data.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15'
    }
PULL_INTERVAL = 3600 * 24
THROTTLE_TIME = 1


def get_apartments_from_google_sheets(local=False):
  if local:
    l.info('Using local spreadsheet data')
    with open(LOCAL_SHEETS_DATA_FN) as f:
      return json.loads(f.read())
  else:
    l.info('Getting data from spreadsheet...')
    sa = SpreadsheetApp(TOKEN_PATH)
    s = sa.open_by_id(SHEET_NAME).sheets[0]
    table = Table(s.get_data_range())

    data = [{k.lower(): v for k, v in a.to_dict().items()}
        for a in table]
    # to save as json, remove datetimes
    for d in data:
      d['start date'] = str(d['start date'])
    with open(LOCAL_SHEETS_DATA_FN, 'w') as f:
      f.write(json.dumps(data))
    return data


def get_updated_dump(a, dumps_by_url):
  '''get a new dump for apartment `a` if outdated'''
  cur_ts = dt.now().timestamp()
  delta = (timedelta(seconds=int(cur_ts - dumps_by_url[a['url']].timestamp))
      if a['url'] in dumps_by_url else 0)

  if not delta or delta.total_seconds() > PULL_INTERVAL:
    l.info(f"Pulling data for: {colored(a['name'], 'green')} after {str(delta)}")
    r = requests.get(a['url'], headers=HEADERS)
    d = Dump(0, a['url'], dt.now().timestamp(), r.status_code, r.text)
    return d

  else:
    l.info(f"Skipping {colored(a['name'], 'green')}: Last pulled {str(delta)} ago")
    return None


def extract_dump(d):
  ''''''
  bs = BeautifulSoup(d.body, features='lxml')
  res = []
  if 'apartments.com' in d.url:
    models = bs.select('#pricingView > div[data-tab-content-id="bed1"] .pricingGridItem')
    for m in models:
      model_name = m.select_one('.priceBedRangeInfo .modelName').text.strip()
      for u in m.select('.unitContainer'):
        # remove these tags
        for s in u.select('.screenReaderOnly'):
          s.extract()

        res.append({
          'model': model_name,
          'unit': u.select_one('.unitColumn').text.strip(),
          'price': u.select_one('.pricingColumn').text.strip(),
          'sqft': u.select_one('.sqftColumn').text.strip(),
          'available': u.select_one('.availableColumn').text.strip(),
          })
  else:
    return None
  return res


def main(args):
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
  apmts = get_apartments_from_google_sheets(local=args.local)
  for a in apmts:
    d = get_updated_dump(a, dumps_by_url)
    if d:
      # new dump pulled
      d.insert(conn, c)
      dumps_by_url[d.url] = d
      time.sleep(THROTTLE_TIME)
    else:
      d = dumps_by_url[a['url']]

    # extract data
    units = extract_dump(d)
    if units is not None:
      l.info(f'Found {len(units)} units')
      for u in units:
        l.info(f'{json.dumps(u)}')



if __name__ == "__main__":
  l.basicConfig(level=l.INFO)
  # l.basicConfig(level=l.DEBUG)

  parser = argparse.ArgumentParser(description='apartment hunter')
  parser.add_argument('--local', action="store_true", help='use local sheets data')
  args = parser.parse_args()
  main(args)
