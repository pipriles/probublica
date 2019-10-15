#!/usr/bin/env python3

import pandas as pd
import requests as rq
import functools
import re
import time
import IPython
import util

from bs4 import BeautifulSoup
from tqdm import tqdm

def tr_with_first_col(e, value):
    return e.name == 'tr' and e.find(string=re.compile(value))

def li_with_bold_name(elem, name):
    return elem.name == 'li' \
        and elem.find('strong', string=re.compile(name))

def extract_text_below(elem):
    strs = elem.find_all(text=True, recursive=False)
    text = strs[-1] if strs else ''
    text = text.rpartition(':')[-1]
    return text.strip() if text else None

def find_bold_name(soup, name):
    fn = functools.partial(li_with_bold_name, name=name)
    elem = soup.find(fn)
    return name, extract_text_below(elem) if elem else None

@util.none_on_error
def parse_year_card(elem):
    data = {}
    
    # Extract Year number
    year = elem.find(class_='left-label')
    year = year.find('h4', attrs=None)
    data['Year'] = year.get_text(strip=True)
    
    # Total revenue
    fn = functools.partial(tr_with_first_col, value='Total Revenue')
    th = elem.find(fn).select_one('th + th')
    data['Total Revenue'] = th.get_text(strip=True)
    
    # Total assets
    fn = functools.partial(tr_with_first_col, value='Total Assets')
    td = elem.find(fn).select_one('td + td')
    data['Total Assets'] = td.get_text(strip=True)
    
    return data

def extract_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    data = {}

    h1 = soup.select_one('.left-col > h1')
    if h1 is None: return []

    data['Name'] = h1.get_text(strip=True)

    spans = soup.select('.left-col > span.small-label')
    meta  = ' | '.join([ x.get_text(strip=True) for x in spans ])
    data['Metadata'] = meta

    k, value = find_bold_name(soup, 'EIN')
    data[k] = value
    
    k, value = find_bold_name(soup, 'Classification')
    data[k] = value
    
    name = 'Nonprofit Tax Code Designation'
    pattern = re.compile(r'^{}'.format(name))
    elem = soup.find('strong', string=pattern)
    text = elem.get_text(strip=True)
    data[name] = text.rpartition(' ')[-1]

    a = soup.find('a', class_='guidestar')
    data['Guidestar URL'] = a['href']
    
    elems = soup.select('.single-filing')
    records = [ parse_year_card(e) for e in elems ]
    records = [ x for x in records if x ]

    if not records:
        keys = ['Year', 'Total Revenue', 'Total Assets']
        missing = dict.fromkeys(keys)
    
    return [ { **data, **x } for x in records ] \
            if records else [{ **data, **missing }]

def fetch_company(url):
    resp = rq.get(url)
    html = resp.text
    return extract_data(html)

def fetch_states_list():
    URL = 'https://projects.propublica.org/nonprofits/'
    resp = rq.get(URL)
    html = resp.text

    soup = BeautifulSoup(html, 'html.parser')
    opts = soup.select('select[name="state[id]"] option')
    return [ o['value'] for o in opts if o['value'] ]

def fetch_org_types():
    URL = 'https://projects.propublica.org/nonprofits/'
    resp = rq.get(URL)
    html = resp.text
    
    soup = BeautifulSoup(html, 'html.parser')
    opts = soup.select('select[name="c_code[id]"] option')
    return [ o['value'] for o in opts if o['value'] ]

def fetch_companies(state='', org=''):
    
    URL = 'https://projects.propublica.org/nonprofits/search'
    params = { 'page': 1, 'state[id]': state, 'c_code[id]': org }
    
    while True:
        resp = rq.get(URL, params=params)
        html = resp.text

        print(resp.url)
        soup = BeautifulSoup(html, 'html.parser')

        if soup.a is None:
            time.sleep(60)
            continue

        href = soup.select('td a')
        yield from [ a['href'] for a in href ]

        next_ = soup.select_one('span.current + span')
        if next_ is None: 
            util.display_raw(html)
            break

        params['page'] += 1
        time.sleep(0.75)

def fetch_states_companies(states):
    for code in states:
        print(code)
        for comp in fetch_companies(state=code):
            yield { 'Company': comp, 'State': code }
            
def fetch_org_companies(orgs):
    for code in orgs:
        print(code)
        for comp in fetch_companies(org=code):
            yield { 'Company': comp, 'Org': code }

def scrape_companies(size=None, offset=0):

    df = pd.read_csv('./companies.csv')
    urls = 'https://projects.propublica.org' + df.Company
    urls = urls.iloc[offset:size]
    results = []

    try:
        rf = pd.read_csv('./results.csv')
        results = rf.to_dict(orient='records')
        scraped = set([ u for u in rf.URL ])
    except FileNotFoundError:
        print('File not found...')
        scraped = set()
    
    try:
        process = tqdm(urls)
        process.write('{} scraped'.format(len(scraped)))
        for url in process:
            process.write(str(url))
            if url in scraped: 
                process.write('Already scraped')
                continue
            records = scrape_company(url)
            results.extend(records)
            time.sleep(0.5)
    except Exception as e:
        process.write(str(e))
        pass
    except KeyboardInterrupt:
        pass

    return results

def scrape_company(url):

    while True:
        # print(url)
        records = fetch_company(url)
        
        if records and records[0]['EIN'] is None:
            time.sleep(60)
            continue
            
        for data in records:
            data['URL'] = url
            
        return records

def scrape_process(urls, size, offset=0):

    index = offset
    url   = urls.iloc[index]

    while True:
        # print(url)
        records = fetch_company(url)
        
        if records and records[0]['EIN'] is None:
            time.sleep(60)
            continue
            
        for data in records:
            data['URL'] = url
            
        yield records
        
        index += 1
        
        if index >= size + offset: 
            break
        
        url = urls.iloc[index]
        time.sleep(0.5)

def main():
    print('Starting scraping process')
    results = scrape_companies()
    cols = [
        'URL', 'Guidestar URL', 'Name', 'Metadata', 
        'EIN', 'Nonprofit Tax Code Designation', 'Classification', 
        'Year', 'Total Revenue', 'Total Assets'
    ]
    cdf = pd.DataFrame(results)
    cdf.drop_duplicates(inplace=True)
    cdf = cdf[cols]
    
    print('Writing results to disk')
    cdf.to_csv('./results.csv', index=None)
        
if __name__ == '__main__':
    main()
