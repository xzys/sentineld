#!/usr/bin/env python3
import json
import logging as l
from sheetfu import SpreadsheetApp, Table
from schema import Dump, Notification, NotificationAction, get_db


TOKEN_PATH = './secrets/sa.json'
with open('./secrets/sheet_name') as f:
  SHEET_NAME = f.read().strip()
LOCAL_SHEETS_DATA_FN = 'sheets_data.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15'
    }
PULL_INTERVAL = 3600
THROTTLE_TIME = 1


def get_google_sheet():
  sa = SpreadsheetApp(TOKEN_PATH)
  return sa.open_by_id(SHEET_NAME)


def get_apartments_from_google_sheets(local=False):
  '''pull data from google sheets and save as JSON'''
  if local:
    l.info('Using local spreadsheet data')
    with open(LOCAL_SHEETS_DATA_FN) as f:
      return json.loads(f.read())
  else:
    l.info('Getting data from spreadsheet...')
    sp = get_google_sheet()
    s = sp.sheets[0]
    table = Table(s.get_data_range())

    data = [{k.lower(): v for k, v in a.to_dict().items()}
        for a in table]
    # to save as json, remove datetimes
    for d in data:
      d['start date'] = str(d['start date'])
    with open(LOCAL_SHEETS_DATA_FN, 'w') as f:
      f.write(json.dumps(data))
    return data
