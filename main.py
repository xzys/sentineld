#!/usr/bin/env python3
import os
import sys
import json
import argparse
import logging as l
from history import sync_price_history
from update import update_dumps, reextract_dumps
from schema import get_db, get_db_hash, DBNAME
from sheets import SHEETS_DATA_FN


def on_event(event, context):
  # setup logging
  root = l.getLogger()
  handler = l.StreamHandler(sys.stdout)
  formatter = CloudLoggingFormatter(fmt="[%(name)s] %(message)s")
  handler.setFormatter(formatter)
  root.addHandler(handler)
  root.setLevel(l.DEBUG)

  # get db from storage
  from google.cloud import storage
  s = storage.Client()
  bucket = s.get_bucket(os.environ['GCS_BUCKET_NAME'])
  l.info(f'Downloading {os.path.basename(DBNAME)} + {os.path.basename(SHEETS_DATA_FN)} from GCS...')
  db_blob = bucket.blob(os.path.basename(DBNAME))
  db_blob.download_to_filename(DBNAME)
  sheets_blob = bucket.blob(os.path.basename(SHEETS_DATA_FN))
  sheets_blob.download_to_filename(SHEETS_DATA_FN)

  h = get_db_hash()
  main(['update'])
  main(['history'])
  newh = get_db_hash()
  if newh != h:
    l.info(f'DB updated with hash:{newh}. Uploading to GCS...')
    db_blob.upload_from_filename(DBNAME)
    sheets_blob.upload_from_filename(SHEETS_DATA_FN)


class CloudLoggingFormatter(l.Formatter):
  '''Produces messages compatible with google cloud logging'''
  def format(self, r: l.LogRecord):
    s = super().format(r)
    return json.dumps(
      {
        'message': s,
        'severity': r.levelname,
        'timestamp': {'seconds': int(r.created), 'nanos': 0},
      }
    )


def main(argv=None):
  action_funcs = {
      'update': update_dumps,
      'reextract': reextract_dumps,
      'history': sync_price_history,
      'migrate': lambda args: get_db(migrate=True),
      }

  parser = argparse.ArgumentParser(description='apartment hunter')
  parser.add_argument('action', choices=action_funcs.keys())
  parser.add_argument('--local',
      action="store_true", help='use local sheets data')
  parser.add_argument('--dry-run',
      action="store_true", help='dont make changes')
  args = parser.parse_args(argv) if argv else parser.parse_args()
  action_funcs[args.action](args)


if __name__ == "__main__":
  l.basicConfig(level=l.INFO)
  main()
