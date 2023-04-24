#!/usr/bin/env python3
import os
import csv
import json
import tempfile
from subprocess import check_output
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


def get_price_data(apmts, conn, c):
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


def get_price_history(conn, c):
  ''''''
  apmts = get_apartments_from_google_sheets(local=True)
  hdata, unit_data = get_price_data(apmts, conn, c)

  dates = [t.date() for k, h in hdata for p, t in h]
  min_date, max_date = min(dates), max(dates)
  vals = [[None for n in range((max_date - min_date).days + 2)]
      for _ in range(len(hdata) + 1)]

  notifications = []
  for j, (k, h) in enumerate(hdata):
    index, model, unit = k
    vals[j+1][0] = f'{apmts[index]["name"]} - {model}/{unit}'

  for n in range((max_date - min_date).days + 1):
    d = min_date + timedelta(days=n)
    vals[0][n+1] = d.strftime('%b %-d %Y')

    for j, (k, h) in enumerate(hdata):
      p, t = next(((p, t) for p, t in h[::-1] if t.date() == d), (None, None))
      vals[j+1][n+1] = p

      if d == max_date and p is not None:
        nf = check_notify_price_change(conn, c, apmts, unit_data, k, p, t)
        if nf:
          notifications.append(nf)

  return vals, notifications


def sync_price_history(args):
  '''sync price history by day to google sheets'''
  conn, c = get_db()
  vals, notifications = get_price_history(conn, c)

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
          n.insert(conn, c)


def filter_lowest_priced_units(vals):
  '''only keep lowest unit in each apartment'''
  keys = [r[0] for r in vals[1:]]
  get_key = lambda s: s.split(' - ')[0]

  lowest_prices = {}
  for r in vals[1:]:
    k = get_key(r[0])
    if not r[-1]:
      continue

    old_price = lowest_prices.get(k, None)
    lowest_prices[k] = min(old_price, r[-1]) if old_price else r[-1]

  new_vals = [vals[0]] + [
      r for r in vals[1:]
      if r[-1] and r[-1] == lowest_prices.get(get_key(r[0]), None)
      ]
  
  return new_vals


def view_price_history(args):
  ''''''
  conn, c = get_db()
  vals, notifications = get_price_history(conn, c)

  vals = filter_lowest_priced_units(vals)

  last_days = 7
  l.info(f'Presenting price history for last {last_days} days')

  tmp = tempfile.NamedTemporaryFile(mode='x', suffix='.csv', delete=False)
  with open(tmp.name, 'w') as f:
    cw = csv.writer(f)
  
    for j, r in enumerate(vals):
      rev_indexes = [0] + list(range(len(r) - last_days, len(r), 1))

      if j == 0:
        cw.writerow([
          r[i][:-5]
          if r[i] and i > 0 else ''
          for i in rev_indexes
          ])

      else:
        cw.writerow([
          (r[i] and str(r[i])) or ''
          for i in rev_indexes
          ])

  check_output(['vd', '--delimiter', ',', tmp.name])
  os.remove(tmp.name)
