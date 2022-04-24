#!/usr/bin/env python3
import os
import json
import sqlite3
import logging as l
from enum import Enum
from dataclasses import dataclass

DBNAME = 'dumps.db'
SCHEMA_UP_SQL = '''
CREATE TABLE IF NOT EXISTS dumps (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  status INTEGER NOT NULL,
  body TEXT NOT NULL,
  extracted JSON 
);

CREATE TABLE IF NOT EXISTS notifications (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  unit TEXT NOT NULL,
  last_notified INTEGER NOT NULL,
  action TEXT NOT NULL,
  data JSON NOT NULL DEFAULT '{}'
);
'''

@dataclass
class Dump:
  id: int
  url: str
  timestamp: int
  status: int
  body: str
  extracted: str

  def insert(self, conn, c):
    return _insert(conn, c, self, 'dumps',
        ('url', 'timestamp', 'status', 'body'))


class NotificationAction(Enum):
  ADDED = 'ADDED'
  REMOVED = 'REMOVED'
  PRICE_INCREASE = 'PRICE_INCREASE'
  PRICE_DECREASE = 'PRICE_DECREASE'


@dataclass
class Notification:
  id: int
  name: str
  unit: int
  last_notified: int
  action: str
  data: int

  def insert(self, conn, c):
    return _insert(conn, c, self, 'notifications',
        ('name', 'unit', 'last_notified', 'action', 'data'))


def _insert(conn, c, dc, table_name, fields):
  '''insert dataclass into database'''
  vals = tuple([dc.__dict__[f] for f in fields])
  c.execute(f'''
      INSERT INTO {table_name} ({','.join(fields)})
      VALUES ({','.join(['?']*len(fields))})
      ''', vals)
  dc.id = c.lastrowid
  conn.commit()


def get_db(migrate=False):
  '''get cursor to db, setup schema if not present'''
  exists = os.path.exists(DBNAME)
  conn = sqlite3.connect(DBNAME)
  c = conn.cursor()
  if not exists or migrate:
    l.info('Database not found/outdated. Setting up schema...')
    c.executescript(SCHEMA_UP_SQL)
  return conn, c
