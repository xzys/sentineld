#!/usr/bin/env python3
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
from sheets import get_google_sheet, get_apartments_from_google_sheets
from schema import Dump, Notification, NotificationAction, get_db


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15'
    }
PULL_INTERVAL = 3600
THROTTLE_TIME = 1


def get_updated_dump(a, dumps_by_url):
  '''get a new dump for apartment `a` if outdated'''
  cur_ts = dt.now().timestamp()
  delta = (timedelta(seconds=int(cur_ts - dumps_by_url[a['url']].timestamp))
      if a['url'] in dumps_by_url else 0)

  if not delta or delta.total_seconds() > PULL_INTERVAL:
    l.info(f"Pulling data for: {colored(a['name'], 'green')} after {str(delta)}")
    r = requests.get(a['url'], headers=HEADERS)
    d = Dump(0, a['url'], dt.now().timestamp(), r.status_code, r.text, None)
    return d

  else:
    l.info(f"Skipping {colored(a['name'], 'green')}: Last pulled {str(delta)} ago")
    return None


def extract_dump(d):
  '''pull important info from page'''
  # bs = BeautifulSoup(d.body, features='lxml')
  bs = BeautifulSoup(d.body, features='html.parser')
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
          'price': int(u.select_one('.pricingColumn').text.strip()
            .replace('$', '').replace(',', '')
            ),
          'sqft': int(u.select_one('.sqftColumn').text.strip()
            .replace(',', '')
            ),
          'available': u.select_one('.availableColumn').text.strip(),
          })
  else:
    return None
  return res


def update_dumps(args):
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
    data = None
    if d:
      # new dump pulled
      data = extract_dump(d)
      d.extracted = json.dumps(data)

      d.insert(conn, c)
      dumps_by_url[d.url] = d
      time.sleep(THROTTLE_TIME)
    else:
      d = dumps_by_url[a['url']]
      if d.extracted:
        data = json.loads(d.extracted)

    if data is not None:
      l.info(f'Found {len(data)} units')
      for u in data:
        l.info(f'{json.dumps(u)}')


def reextract_dumps(args):
  conn, c = get_db()

  res = c.execute('''SELECT * FROM dumps''').fetchall()
  for r in res:
    d = Dump(*r)
    data = extract_dump(d)
    c.execute('''UPDATE dumps SET extracted = ? WHERE id = ?''',
        (json.dumps(data), d.id))
  conn.commit()
  l.info(f'Re-extracted {len(res)} dumps!')
