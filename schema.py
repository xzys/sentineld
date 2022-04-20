#!/usr/bin/env python3
import os
import sqlite3
from dataclasses import dataclass

DBNAME = 'dumps.db'
SCHEMA_UP_SQL = '''
CREATE TABLE dumps IF NOT EXISTS (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  status INTEGER NOT NULL,
  body TEXT NOT NULL
);
'''

@dataclass
class Dump:
  id: int
  url: str
  timestamp: int
  status: int
  body: str

  def insert(self, conn, c):
    '''insert into database'''
    c.execute('INSERT INTO dumps (url, timestamp, status, body) values (?,?,?,?)',
        (self.url, self.timestamp, self.status, self.body))
    self.id = c.lastrowid
    conn.commit()


def get_db():
  '''get cursor to db, setup schema if not present'''
  exists = os.path.exists(DBNAME)
  conn = sqlite3.connect(DBNAME)
  c = conn.cursor()
  if not exists:
    l.info('Database not found. Setting up schema...')
    c.execute(SCHEMA_UP_SQL)
  return conn, c
