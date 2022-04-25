#!/usr/bin/env python3
import os
import json
import logging as l
from termcolor import colored
from datetime import datetime as dt
from datetime import timedelta
from update import extract_dump
from sheets import get_google_sheet, get_apartments_from_google_sheets
from schema import Dump, Notification, NotificationAction, get_db
from notify import create_email, send_email, check_notify_price_change


EMAIL_RECIPIENTS = None
if 'EMAIL_RECIPIENTS' in os.environ:
  EMAIL_RECIPIENTS = os.environ['EMAIL_RECIPIENTS'].split(',')
else:
  with open('secrets/recipients') as f:
    EMAIL_RECIPIENTS = f.read().strip().split('\n')


def get_price_history(apmts, conn, c):
  hist, unit_data = {}, {}
  for i, a in enumerate(apmts):
    res = c.execute('''
        SELECT t1.*
        FROM dumps t1
        WHERE status = 200 AND url = ?
        ORDER BY timestamp
        ''', (a['url'],))

    for r in res:
      d = Dump(*r)
      data = json.loads(d.extracted)

      for u in data:
        k = (i, u['model'], u['unit'])
        if k not in unit_data:
          unit_data[k] = u

        if k not in hist:
          hist[k] = []
        # elif hist[k][-1][0] == u['price']:
        #   continue

        hist[k].append((
          u['price'],
          dt.fromtimestamp(d.timestamp),
          ))

  hdata = sorted(hist.items(), key=lambda p: p[0])
  return hdata, unit_data


def sync_price_history(args):
  '''sync price history by day to google sheets'''
  conn, c = get_db()
  apmts = get_apartments_from_google_sheets(True)
  hdata, unit_data = get_price_history(apmts, conn, c)

  dates = [t.date() for k, h in hdata for p, t in h]
  min_date, max_date = min(dates), max(dates)
  vals = [[None for n in range((max_date - min_date).days + 3)]
      for _ in range(len(hdata) + 1)]

  notifications = []
  for j, (k, h) in enumerate(hdata):
    index, model, unit = k
    vals[j+1][0] = apmts[index]['name']
    vals[j+1][1] = f'{model}/{unit}'

  for n in range((max_date - min_date).days + 1):
    d = min_date + timedelta(days=n)
    vals[0][n+2] = d.strftime('%b %-d %Y')

    for j, (k, h) in enumerate(hdata):
      p, t = next(((p, t) for p, t in h[::-1] if t.date() == d), (None, None))
      vals[j+1][n+2] = p

      if d == max_date and p is not None:
        nf = check_notify_price_change(conn, c, apmts, unit_data, k, p, t)
        if nf:
          notifications.append(nf)

  if not args.dry_run:
    l.info('Updating spreadsheet')
    sp = get_google_sheet()
    s = sp.sheets[1]
    s.get_data_range().set_value(None)

    dr = s.get_range(row=1, column=1,
        number_of_row=len(vals), number_of_column=len(vals[0]))
    dr.set_values(vals)
    sp.commit()

  if len(notifications):
    l.info(f'Sending {len(notifications)} notifications')

    send_email(EMAIL_RECIPIENTS, notifications)

    for n in notifications:
      if not args.dry_run:
        n.insert(conn, c)
