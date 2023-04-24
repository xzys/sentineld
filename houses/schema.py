#!/usr/bin/env python3
import os
from enum import StrEnum
import sqlite3
from dataclasses import dataclass, field, fields
import logging as l


DBNAME = 'houses.db'


class HomeType(StrEnum):
  single_family= 'SINGLE_FAMILY'


class Status(StrEnum):
  for_sale = 'FOR_SALE'


@dataclass(kw_only=True)
class Property:
  id: int = field(metadata={'PRIMARY KEY': True}, init=False, default=None)
  price: float
  zillow_estimate: float = field(metadata={'NOT NULL': False})
  rent_estimate: float = field(metadata={'NOT NULL': False})
  tax_addressed_value: float = field(metadata={'NOT NULL': False})
  price_reduction: str = field(metadata={'NOT NULL': False})
  zpid: int
  date: str = field(metadata={'DEFAULT': 'CURRENT_DATE'}, init=False, default=None)

  beds: int
  bath: float
  area: int
  home_type: HomeType

  status: Status
  image_url: str
  detail_url: str

  latitude: float
  longitude: float

  address: str = field(metadata={'NOT NULL': False})
  city: str
  state: str
  zipcode: str


CREATE_TABLE_SQL = '''CREATE TABLE IF NOT EXISTS {} (\n  {}\n)'''

TYPE_SQL_MAP = {
    'int': 'INTEGER',
    'float': 'REAL',
    'str': 'TEXT',
    }

def create_table_sql(cls) -> str:
  fields_data: list[str] = []

  for f in fields(cls):
    type_name = f.type.__name__

    # default to text, but FYI there could be int enums too...
    sql_type = TYPE_SQL_MAP.get(type_name, 'TEXT')

    # by default all fields are NOT NULL
    metadata: dict[str, bool | str] = dict({'NOT NULL': True}, **f.metadata)
    extra_args = [
        f'{k}{"" if v == True else " "+str(v)}'
        for k, v in metadata.items()
        if v]

    fields_data.append(f'{f.name} {sql_type}{" ".join([""] + extra_args)}')

  return CREATE_TABLE_SQL.format(
      cls.__name__.lower(),
      ',\n  '.join(fields_data))


INSERT_SQL = '''INSERT INTO {} ({}) VALUES ({})'''

def insert_sql(ins) -> tuple[str, list]:
  # look up values in the dict representation
  column_names = [f.name for f in fields(ins) if f.init]
  values = [ins.__dict__[name] for name in column_names]
  # automatically generate number of '?'
  sql = INSERT_SQL.format(
      ins.__class__.__name__.lower(),
      ', '.join(column_names),
      ', '.join(['?']*len(values)))

  return sql, values


def initialize_db(conn, c):
  c.execute(create_table_sql(Property))
  conn.commit()


def get_db(migrate=False):
  '''get cursor to db, setup schema if not present'''
  exists = os.path.exists(DBNAME)
  conn = sqlite3.connect(DBNAME)
  c = conn.cursor()
  if not exists or migrate:
    l.info('Database not found/outdated. Setting up schema...')
    initialize_db(conn, c)
  return conn, c
