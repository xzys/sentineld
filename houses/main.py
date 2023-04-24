#!/usr/bin/env python3
import argparse
import time
import json
import logging as l
import undetected_chromedriver as uc
from selenium.webdriver.remote.webdriver import By
from selenium.webdriver.support.wait import WebDriverWait

from schema import get_db, Property, insert_sql


ZILLOW_URL = 'https://www.zillow.com/homes/for_sale'


find_elem = lambda driver, xpath: WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.XPATH, xpath))

def test_save():
  conn, c = get_db()

  with open('/tmp/a.json') as f:
    data = json.loads(f.read())
    results = {'cat1': {'searchResults': {'mapResults': data}}}

    save_results(conn, c, results)

def save_results(conn, c, body):
  res = body['cat1']['searchResults']['mapResults']
  # with open('/tmp/a.json', 'w') as out:
  #   res = body['cat1']['searchResults']['mapResults']
  #   out.write(json.dumps(res))
  #   l.info(f'saved {len(res)} results')
  required_fields = ['zpid', 'beds', 'baths', 'area']

  for r in res:
    if not all(f in r and r[f] is not None for f in required_fields):
      continue
    
    home_info = r.get('hdpData', {}).get('homeInfo', {})
    price_str = r['price'].replace('$', '').replace(',', '').lower()
    price = int(price_str[:-1]) * 1000 if price_str.endswith('k') else int(price_str) 

    p = Property(
        price=price,
        zillow_estimate=home_info.get('zestimate'),
        rent_estimate=home_info.get('rentZestimate'),
        tax_addressed_value=home_info.get('taxAddressedValue'),
        price_reduction=home_info.get('priceReduction'),
        zpid=int(r['zpid']),
        beds=r['beds'],
        bath=r['baths'],
        area=r['area'],
        home_type=home_info.get('homeType'),
        status=r['statusType'],
        image_url=r['imgSrc'],
        detail_url=r['detailUrl'],
        latitude=r['latLong']['latitude'],
        longitude=r['latLong']['longitude'],
        address=r['address'] if r['address'] != '--' else None,
        city=home_info.get('city'),
        state=home_info.get('state'),
        zipcode=home_info.get('zipcode'),
      )

    l.info(f'inserting property from {p.detail_url}')
    sql, values = insert_sql(p)
    print(p)
    print(sql)
    c.execute(sql, values)

  conn.commit()



def update(args):
  conn, c = get_db()

  l.info('starting driver...')
  driver = uc.Chrome(
      enable_cdp_events=True
      )
  save_next_search = False

  def response_received(e):
    nonlocal save_next_search
    try:
      if not save_next_search:
        return

      req_url = e['params']['response']['url']
      if not 'GetSearchPageState.htm' in req_url:
        return

      l.info(f'found search request. getting results...')
      req_id = e['params']['requestId']

      resp = driver.execute_cdp_cmd('Network.getResponseBody', {
        'requestId': req_id
        })
      save_results(conn, c, json.loads(resp['body']))

      save_next_search = False
    except Exception as e:
      print('error', e)

  driver.add_cdp_listener('Network.responseReceived', response_received)
  
  # main routine
  if not args.not_exact:
    save_next_search = True

  # url = f'{ZILLOW_URL}/{location}'
  # url = ZILLOW_URL
  search_query = {
    'mapBounds': {
      'west': -81.57849441503905,
      'east': -81.03947769140623,
      'south': 28.35053895722964,
      'north': 28.868348974554998
    },
    'isMapVisible': True,
    'filterState': {
      'sort': { 'value': 'globalrelevanceex' },
      'ah': { 'value': True }
    },
    'isListVisible': True,
    'mapZoom': 11
  }
  url = f'{ZILLOW_URL}/?searchQueryState={json.dumps(search_query)}'

  driver.get(url) 

  # any type of automated activity on the page seems to cause bot detection
  # so put search query in URL

  # l.info(f'initiating search for {location}')
  # el = find_elem(driver, 'div[id="srp-search-box"] input')
  # el.send_keys(f'{location}\n')

  # if not_exact:
  #   time.sleep(1)

  #   el = find_elem(driver, 'div.map-layer-controls [data-test="remove-boundary"]')
  #   l.info('found remove boundry button')
  #   save_next_search = True
  #   el.click()

  WebDriverWait(driver, timeout=10).until(lambda d: save_next_search == False)
  driver.quit()


def main():
  action_funcs = {
      'update': update,
      }

  parser = argparse.ArgumentParser(description='house hunter')
  parser.add_argument('action', required=True, choices=action_funcs.keys())
  parser.add_argument('--location', help='location search query')
  parser.add_argument('--not-exact', action='store_true', default=False)
  args = parser.parse_args()

  action_funcs[args.action](args)


if __name__ == '__main__':
  l.getLogger().setLevel(l.INFO)
  main()
