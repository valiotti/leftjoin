#!/usr/bin/python
import requests
from datetime import timedelta, datetime
import urllib.parse
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from lxml.html import fromstring
from clickhouse_driver import Client
from clickhouse_driver import errors
import numpy as np
from funcs import check_title, get_skills_row, parse_salary, get_sheetname, create_table

logger = logging.getLogger('indeed_vacancies')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('indeed.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

countries = [
#'ar', 'au', 'at', 'bh', 'be', 
'br', 'ca', 'cl', 'cn', 'co', 'cz', 'dk', 'fi', 'fr', 'de', 'gr', 'hk', 'hu',
             'in', 'id', 'ie', 'il', 'it', 'jp', 'kr', 'kw', 'lu', 'my', 'mx', 'nl', 'nz', 'no', 'om', 'pk', 'pe', 'ph',
             'pl', 'pt', 'qt', 'ro', 'ru', 'sa', 'sg', 'za', 'es', 'se', 'ch', 'tw', 'th', 'tr', 'ae', 'gb', 'us', 've',
             'vn']

countries_full = [
#'Argentina', 'Australia', 'Austria', 'Bahrain', 'Belgium', 
'Brazil', 'Canada', 'Chile', 'China',
                  'Colombia', 'Czech Republic', 'Denmark', 'Finland', 'France', 'Germany', 'Greece', 'Hong Kong',
                  'Hungary', 'India', 'Indonesia', 'Ireland', 'Israel', 'Italy', 'Japan', 'Korea', 'Kuwait',
                  'Luxembourg', 'Malaysia', 'Mexico', 'Netherlands', 'New Zealand', 'Norway', 'Oman', 'Pakistan',
                  'Peru', 'Philippines', 'Poland', 'Portugal', 'Qatar', 'Romania', 'Russia', 'Saudi Arabia',
                  'Singapore', 'South Africa', 'Spain', 'Sweden', 'Switzerland', 'Taiwan', 'Thailand', 'Turkey',
                  'United Arab Emirates', 'United Kingdom', 'United States', 'Venezuela', 'Vietnam']

countries_dict = dict(zip(countries, countries_full))

ua = UserAgent()

headers = {
    'User-Agent': ua['google chrome']
}


def get_proxy_list():
    url_pr = 'https://free-proxy-list.net/anonymous-proxy.html'
    response = requests.get(url_pr)
    parser = fromstring(response.text)
    proxies_out = []
    for line in parser.xpath('//tbody/tr')[:40]:
        if line.xpath('.//td[7][contains(text(),"yes")]'):
            proxy = ":".join([line.xpath('.//td[1]/text()')[0], line.xpath('.//td[2]/text()')[0]])
            try:
                t = requests.get("https://www.google.com/", proxies={"http": proxy, "https": proxy}, timeout=5,
                                 headers=headers)
                if t.status_code == requests.codes.ok and proxy != '':
                    proxies_out.append(proxy)
            except:
                pass
    return list(set(proxies_out))


def raw_date_to_str(raw_date):
    raw_date = raw_date.lower()
    if '+' in raw_date or "более" in raw_date:
        delta = timedelta(days=32)
        return (datetime.now() - delta).strftime("%Y-%m-%d")
    else:
        parts = raw_date.split()
        for part in parts:
            if part.isdigit():
                delta = timedelta(days=part.isdigit())
                return (datetime.now() - delta).strftime("%Y-%m-%d")
    return ""


def get_page(pxs, updated_url):
    proxy = pxs[0]
    proxy_dict = {"http": proxy, "https": proxy}
    logger.info(f'try with proxy: {proxy}')
    while True:
        try:
            return req.get(updated_url, headers=headers, proxies=proxy_dict, timeout=15)
        except (requests.exceptions.ProxyError, requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout, requests.exceptions.SSLError, requests.exceptions.ConnectionError):
            pxs.remove(proxy)
            logger.info(f'try with proxy {proxy}')
            if len(pxs) == 0:
                logger.info('Sleeping for 5 minutes because there is no available proxy')
                time.sleep(300)
                pxs = get_proxy_list()
                while len(pxs) == 0:
                    pxs = get_proxy_list()
                    logger.info('Sleeping for 1 minute between parsing proxy')
                    time.sleep(60)
            proxy = pxs[0]
            proxy_dict = {"http": proxy, "https": proxy}


def get_jobs_count(raw_jobs_count, coun):
    raw_jobs_count = raw_jobs_count.split()
    jobs = raw_jobs_count[-2]
    if coun == 'gr':
        jobs = raw_jobs_count[-3]
    elif coun == 'kr':
        jobs = ''
        for let in raw_jobs_count[-1]:
            if let.isdigit():
                jobs += let
    elif coun == 'ro':
        jobs = raw_jobs_count[3]
    elif coun == 'vn':
        jobs = raw_jobs_count[1].split('/')[-1]
    if ',' in jobs:
        jobs = jobs.replace(',', '')
    if '.' in jobs:
        jobs = jobs.replace('.', '')
    return int(jobs)


if __name__ == "__main__":
    start = time.time()
    link_template = "https://{}.indeed.com{}"
    logger.info('Getting proxies')
    proxies = get_proxy_list()
    while len(proxies) == 0:
        proxies = get_proxy_list()
        time.sleep(60)
    try:
        client = Client(
            host='localhost',
            user='default',
            port='9000',
            password='',
            database='indeed'
        )
        client.execute(create_table)
    except Exception as e:
        logging.error('CAN NOT CREATE CLIENT OR TABLE', e)
        exit(0)
    row_ind = client.execute('select max(row_idx) from indeed.vacancies')[0][0] + 1
    data = pd.read_csv("jobs.csv")
    working_data = data[data["Язык"] == "en"]
    for country in countries:
        if country == "ru":
            working_data = data[data["Язык"] == "ru"]
        with requests.Session() as req:
            for _, request in working_data.iterrows():
                query = request["Ключевое слово"][1:]
                location = countries_dict[country]
                params = {
                    'q': query,
                    'filter': 0,
                    'start': 0,
                }
                if country == 'us':
                    url = f"https://indeed.com/jobs?" + urllib.parse.urlencode(params)
                else:
                    url = f"https://{country}.indeed.com/jobs?" + urllib.parse.urlencode(params)
                logger.info(f'before first usage. query = {query}, country = {country}')
                page = get_page(proxies, url)
                soup = BeautifulSoup(page.text, "html.parser")
                with open('page.txt', 'w') as f:
                    f.write(soup.prettify())
                elem = soup.find(class_="bad_query")
                if elem is not None:
                    print(f'No jobs for {query} in {location}')
                    continue

                jobs_count_raw = soup.find('div', {'id': 'searchCountPages'}).text
                jobs_count = get_jobs_count(jobs_count_raw, country)

                counter = 0
                logger.info(f'{jobs_count} jobs for query {query} in {location}')
                while counter < jobs_count:
                    params["start"] += 10
                    all_jobs = soup.find_all('div', {'class': 'jobsearch-SerpJobCard'})
                    for item in all_jobs:
                        counter += 1
                        print(f'{counter} out of {jobs_count}')
                        try:
                            job_id = item['id'].split('_')[1]
                        except:
                            job_id = ""
                        try:
                            if client.execute(
                                    f"select count(1) from indeed.vacancies where job_id='{job_id}' "
                                    f"AND query_string='{query}'")[0][0] != 0:
                                continue
                        except Exception as e:
                            print(e)
                            continue
                        try:
                            title = item.find('a', {'class': 'jobtitle'}).text
                            title = title.replace('\n','')
                            if not check_title(title, query):
                                logger.info(f'{title} is not suitable for query {query}. skip')
                                continue
                        except:
                            title = ""
                        try:
                            if country == 'us':
                                link = link_template.format(country, item.find('a', {'class': 'jobtitle'})["href"])
                            else:
                                link = link_template.format(country, item.find('a', {'class': 'jobtitle'})["href"])
                        except:
                            link = ""
                        try:
                            company = item.find('span', {'class': 'company'}).text
                            company = company.replace('\n', '')
                        except:
                            company = ""
                        try:
                            city = item.find(class_='location').text
                        except:
                            city = ""
                        try:
                            summary = item.find('div', {'class': 'summary'}).text
                            skills = get_skills_row(summary)
                        except:
                            summary = ""

                        try:
                            date = item.find('span', {'class': 'date'}).text
                            date = raw_date_to_str(date)
                        except:
                            date = ""

                        try:
                            salary = item.find('span', {'class': 'salaryText'}).text
                            month_from_USD, month_to_USD, month_from_bm, month_to_bm = parse_salary(salary, location)
                            year_from_USD = month_from_USD * 12
                            year_to_USD = month_to_USD * 12
                            year_from_bm = month_from_bm * 12
                            year_to_bm = month_to_bm * 12
                        except AttributeError:
                            salary = ''
                            month_from_USD = np.nan
                            month_to_USD = np.nan
                            month_from_bm = np.nan
                            month_to_bm = np.nan
                            year_from_USD = np.nan
                            year_to_USD = np.nan
                            year_from_bm = np.nan
                            year_to_bm = np.nan

                        try:
                            if item.find('span', {'class': 'iaLabel'}).text is not None:
                                easy_apply = "1"
                        except AttributeError:
                            easy_apply = "0"

                        try:
                            rating = item.find('span', {'class': 'ratingsContent'}).text
                        except AttributeError:
                            rating = ""

                        try:
                            if item.find('span', {'class': 'remote'}).text is not None:
                                remote = "1"
                        except AttributeError:
                            remote = "0"

                        added_date = datetime.now().date()
                        sheet = get_sheetname(added_date)
                        added_date = datetime.strftime(datetime.now(), '%Y-%m-%d')

                        if date == '':
                            date = added_date

                        row = [row_ind, query, location, title, company, city, date, easy_apply, rating, remote,
                               job_id, link, sheet, skills, added_date, month_from_USD, month_to_USD, year_from_USD,
                               year_to_USD, month_from_bm, month_to_bm, year_from_bm, year_to_bm]

                        for i, r in enumerate(row):
                            if type(r) == 'str':
                                if '\n' in r:
                                    row[i] = r.replace('\n', '')
                        row_ind += 1

                        print(f'insert into indeed.vacancies values {tuple(row)}')
                        try:
                            client.execute(f'insert into indeed.vacancies values {tuple(row)}')
                        except errors.ServerException:
                            logger.error(f'Cannot insert row {tuple(row)}')
                        if counter >= jobs_count or len(row) == 0:
                            break
                    if country == 'us':
                        url = f"https://indeed.com/jobs?" + urllib.parse.urlencode(params)
                    else:
                        url = f"https://{country}.indeed.com/jobs?" + urllib.parse.urlencode(params)
                    if counter < jobs_count:
                        while len(proxies) == 0:
                            proxies = get_proxy_list()
                            time.sleep(60)
                        logger.info(f'before second usage. counter = {counter}, query = {query}, country = {country}')
                        page = get_page(proxies, url)
                        soup = BeautifulSoup(page.text, "html.parser")
    logger.info(f"FINISHED. Overall time {time.time() - start}")
