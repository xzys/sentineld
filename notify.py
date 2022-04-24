#!/usr/bin/env python3
import smtplib
import json
import time
from schema import Notification, NotificationAction
from bs4 import BeautifulSoup, Tag
from email.message import EmailMessage


with open('secrets/email_creds') as f:
  lines = f.readlines()
  EMAIL_SENDER = lines[0].strip()
  EMAIL_PASSWORD = lines[1].strip()


def check_notify_price_change(conn, c, apmts, unit_data, k, p, t):
  '''notify user on price change'''
  index, model, unit = k
  r = c.execute('''
      SELECT *
      FROM notifications
      WHERE name = ? AND unit = ?
      ORDER BY last_notified DESC
      LIMIT 1
      ''', (apmts[index]['name'], unit)).fetchone()

  action = None
  data = {
      'price': p,
      'sqft': unit_data[k]['sqft'],
      'available': unit_data[k]['available'],
      }
  if r:
    n = Notification(*r)
    nd = json.loads(n.data)
    if nd['price'] != p:
      data['last_price'] = nd['price']
      action = (
          NotificationAction.PRICE_INCREASE if p > nd['price'] else
          NotificationAction.PRICE_DECREASE)
  else:
    action = NotificationAction.ADDED

  if action:
    return Notification(0, apmts[index]['name'], unit, int(t.timestamp()), action.name, json.dumps(data))
  return None


def create_email(notifications):
  # bs = BeautifulSoup()
  msgs = []
  for n in notifications:
    d = json.loads(n.data)
    msg = (f'Unit {n.unit} at {n.name} ({d["sqft"]} sq.ft. / {d["available"]}) ' + (
        f'added at ${d["price"]}!' if n.action == NotificationAction.ADDED.name else
        f'increased from ${d["last_price"]} to ${d["price"]}!' if n.action == NotificationAction.PRICE_INCREASE.name else
        f'decreased from ${d["last_price"]} to ${d["price"]}!' if n.action == NotificationAction.PRICE_DECREASE.name else
        'removed!'
        ))
    msgs.append(msg)
    # d = bs.new_tag('div')
    # d.string = msg
    # bs.append(d)
  # return str(bs)
  return '\n'.join(msgs)


def send_email(recipients, notifications):
  content = create_email(notifications)
  msg = EmailMessage()
  msg.set_content(content)
  msg['Subject'] = f'{len(notifications)} New Apartment Updates!'
  msg['From'] = EMAIL_SENDER
  msg['To'] = ', '.join(recipients)

  s = smtplib.SMTP('smtp.gmail.com', 587)
  s.ehlo()
  s.starttls()
  s.login(EMAIL_SENDER, EMAIL_PASSWORD)
  s.send_message(msg)
  s.quit()
