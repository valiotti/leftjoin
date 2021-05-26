#!/usr/bin/python
import numpy as np
import pandas as pd
from currency_converter import CurrencyConverter
from datetime import date
import re

create_table = '''CREATE TABLE if not exists indeed.vacancies (
    row_idx UInt16,
    query_string String,
    country String,
    title String,
    company String,
    city String,
    job_added Date,
    easy_apply UInt8,
    company_rating Nullable(Float32),
    remote UInt8,
    job_id String,
    job_link String,
    sheet String,
    skills String,
    added_date Date,
    month_salary_from_USD Float64,
    month_salary_to_USD Float64,
    year_salary_from_USD Float64,
    year_salary_to_USD Float64,
    month_salary_from_bigmac Float64,
    month_salary_to_bigmac Float64,
    year_salary_from_bigmac Float64,
    year_salary_to_bigmac Float64
)
ENGINE = ReplacingMergeTree
ORDER BY row_idx
SETTINGS index_granularity = 8192'''

jobs_templates = {
    'Руководитель data science OR Head of data science': [r'.*\bРуководитель\b.*\bdata\b.*\bscience\b.*',
                                                          r'.*\bHead\b.*\bof\b.*\bdata\b.*\bscience\b.*'],
    'Head of data science': [r'.*\bHead\b.*\bof\b.*\bdata\b.*\bscience\b.*'],
    'Руководитель направления предиктивной аналитики': [
        r'.*\bРуководитель\b.*\bнаправления\b.*\bпредиктивной\b.*\bаналитики\b.*'],
    'Аналитик BI OR BI Analyst': [r'.*\bАналитик\b.*\bBI\b.*', r'.*\bBI\b.*\bAnalyst\b.*',
                                  r'.*\bbusiness\b.*\bintelligence\b.*\banalyst\b.*'],
    'BI Analyst': [r'.*\bBI\b.*\bAnalyst\b.*', r'.*\bbusiness\b.*\bintelligence\b.*\banalyst\b.*'],
    'Директор по визуализации OR Директор по аналитической отчетности OR Business Intelligence Officer':
        [r'.*\bДиректор\b.*\bпо\b.*\bвизуализации\b.*', r'.*\bДиректор\b.*\bпо\b.*\bаналитической\b.*\bотчетности\b.*',
         r'.*\bBusiness\b.*\bIntelligence\b.*\bOfficer\b.*'],
    'Аналитик баз данных OR Database analyst': [r'.*\bАналитик\b.*\bбаз\b.*\bданных\b.*',
                                                r'.*\bDatabase\b.*\banalyst\b.*'],
    'Database analyst': [r'.*\bDatabase\b.*\banalyst\b.*', r'.*\bdata\b.*\bbase\b.*\banalyst\b.*'],
    'Разработчик BI OR BI Developer': [r'.*\bРазработчик\b.*\bBI\b.*', r'.*\bBI\b.*\bDeveloper\b.*',
                                       r'.*\bbusiness\b.*\bintelligence\b.*\bdeveloper\b.*'],
    'BI Developer': [r'.*\bBI\b.*\bDeveloper\b.*', r'.*\bbusiness\b.*\bintelligence\b.*\bdeveloper\b.*'],
    'Инженер баз данных OR DWH Engineer': [r'.*\bИнженер\b.*\bбаз\b.*\bданных\b.*', r'.*\bDWH\b.*\bEngineer\b.*'],
    'DWH Engineer': [r'.*\bDWH\b.*\bEngineer\b.*'],
    'Продуктовый аналитик OR Product Analyst': [r'.*\bПродуктовый\b.*\bаналитик\b.*', r'.*\bProduct\b.*\bAnalyst\b.*'],
    'Product Analyst': [r'.*\bProduct\b.*\bAnalyst\b.*'],
    'Data scientist OR Machine Learning specialist': [r'.*\bData\b.*\bscientist\b.*',
                                                      r'.*\bMachine\b.*\bLearning\b.*\bspecialist\b.*'],
    'Аналитик данных OR Data analyst': [r'.*\bАналитик\b.*\bданных\b.*', r'^(?!.*junior).*\bdata\b.*\banalyst\b'],
    'Data analyst': [r"^(?!.*junior).*\bdata\b.*\banalyst\b"],
    'Marketing Analyst': [r'.*\bMarketing\b.*\bAnalyst\b.*'],
    'Business Intelligence Officer': [r'.*\bBusiness\b.*\bIntelligence\b.*\bOfficer.*'],
    'Marketing Analyst OR Маркетинговый аналитик': [r'.*\bМаркетинговый\b.*\bаналитик\b.*',
                                                    r'.*\bMarketing\b.*\bAnalyst\b.*'],
    'Бизнес аналитик OR Business Analyst': [r'.*\bБизнес\b.*\bаналитик\b.*', 'Business Analyst'],
    'Business Analyst': [r'.*\bBusiness\b.*\bAnalyst\b.*'],
    'Junior Data Analyst': [r'.*\bJunior\b.*\bData\b.*\bAnalyst\b.*'],
    'Head of analytics OR Head of BI': [r'.*\bHead\b.*\bof\b.*\banalytics\b.*', r'.*\bHead\b.*\bof\b.*\bBI\b.*',
                                        r'.*\bHead\b.*\bof\b.*\bbusiness\b.*\bintelligence\b.*']
}

skills = ["python", "tableau", "etl", "power bi", "d3.js", "qlik", "qlikview", "qliksense",
          "redash", "metabase", "numpy", "pandas", "congos", "superset", "matplotlib", "plotly",
          "airflow", "spark", "luigi", "machine learning", "amplitude", "sql", "nosql", "clickhouse",
          'sas', "hadoop", "pytorch", "tensorflow", "bash", "scala", "git", "aws", "docker",
          "linux", "kafka", "nifi", "ozzie", "ssas", "ssis", "redis", 'olap', ' r ', 'bigquery', 'api', 'excel']

periods = {
    'a year': 'year',
    'per jaar': 'year',
    'por ano': 'year',
    'par an': 'year',
    "all'anno": 'year',
    'pro Jahr': 'year',
    'một năm': 'year',
    'al año': 'year',
    '年': 'year',
    'pe an': 'year',
    'rocznie': 'year',
    'a month': 'month',
    'al mese': 'month',
    'al mes': 'month',
    'por mês': 'month',
    'per maand': 'month',
    'měsíčně': 'month',
    'par mois': 'month',
    'pro Monat': 'month',
    'per bulan': 'month',
    'ต่อเดือน': 'month',
    'в месяц': 'month',
    'một tháng': 'month',
    'شهر': 'month',
    '月薪': 'month',
    'miesięcznie': 'month',
    'a day': 'day',
    'per dag': 'day',
    'par jour': 'day',
    'an hour': 'hour',
    'za hodinu': 'hour',
    'par heure': 'hour',
    'pro Stunde': 'hour',
    'per timme': 'hour',
    'pe oră': 'hour',
    'một giờ': 'hour',
    'por hora': 'hour',
    'za godzinę': 'hour',
    'per uur': 'hour',
    'a week': 'week',
    'per week': 'week',
    'par semaine': 'week',
    'บาทต่อสัปดาห์': 'week',
    'por semana': 'week',
}

month_per = {
    'hour': 160,
    'day': 20,
    'week': 4,
    'month': 1,
    'year': 1 / 12
}

missed_currencies = {
    'VND': 4e-5,
    'PEN': 0.27664,
    'AED': 0.272300
}

bm_idx = pd.read_csv('bm_index.csv')
bm = dict(zip(bm_idx['country'], bm_idx['cost']))


def check_title(title, query):
    query = query.replace('"', '')
    title = title.replace('"', '')
    result = False
    for temp in jobs_templates[query]:
        result = bool(re.search(temp, title, re.IGNORECASE))
        if result:
            break
    return result


def get_skills_row(summary):
    summary = summary.lower()
    row = []
    for sk in skills:
        if sk in summary:
            row.append(sk)
    return ','.join(row)


# разбиваем строку с зарплатой на вилку, валюту и период
def get_currency(salary_string):
    if 'R$' in salary_string:
        return 'BRL', 'R$'
    elif '$' in salary_string:
        return 'USD', '$'
    elif '€' in salary_string:
        return 'EUR', '€'
    # чешские кроны
    elif 'Kč' in salary_string:
        return 'CZK', 'Kč'
    # индийские рупии
    elif '₹' in salary_string:
        return 'INR', '₹'
    # индонезийская рупия
    elif 'Rp.' in salary_string:
        return "IDR", 'Rp.'
    elif '원' in salary_string:
        return "KRW", '원'
    elif 'руб' in salary_string:
        return "RUB", 'руб'
    elif '₽' in salary_string:
        return "RUB", '₽'
    elif '円' in salary_string:
        return "JPY", '円'
    elif 'VNĐ' in salary_string:
        return "VND", 'VNĐ'
    elif 'บาท' in salary_string:
        return "THB", 'บาท'
    elif 'kr' in salary_string:
        return "SEK", 'kr'
    elif '元' in salary_string:
        return "CNY", '元'
    elif 'RON' in salary_string:
        return "RON", 'RON'
    elif 'zł' in salary_string:
        return "PLN", 'zł'
    elif 'Rs' in salary_string:
        return "INR", 'Rs'
    elif 'S/.' in salary_string:
        return "PEN", 'S/.'
    elif 'PHP' in salary_string:
        return "PHP", 'PHP'
    elif 'R' in salary_string:
        return "ZAR", 'R'
    elif 'CHF' in salary_string:
        return "CHF", 'CHF'
    elif 'AED' in salary_string:
        return "AED", 'AED'
    elif '£' in salary_string:
        return "GBP", '£'

    elif 'CHF' in salary_string:
        return "CHF", 'CHF'
    elif 'CHF' in salary_string:
        return "CHF", 'CHF'
    elif 'CHF' in salary_string:
        return "CHF", 'CHF'
    else:
        return '', ''


def salary_to_int(salary, per, country):
    if salary == '':
        return np.nan
    try:
        if salary.count('.') > 1 or (salary.count('.') == 1 and per != 'hour'):
            salary = salary.replace('.', '')
        if country in ('Chile', 'Colombia') and salary.count('.') == 2:
            salary.replace('.', '')
            salary = float(salary) / 1000
        salary = salary.replace(',', '')
        salary = salary.replace(' ', '')
        salary = salary.replace('’', '')
        return float(salary)
    except ValueError as e:
        print("ERROR IN SALARY TO INT", e)
        print(salary)
        print(type(salary))


def calculate_month_salary(salary_from, salary_to, period):
    return salary_from * month_per[period], salary_to * month_per[period]


def convert_salary(amount_from, amount_to, currency):
    conv = CurrencyConverter()
    try:
        return conv.convert(amount_from, currency, 'USD', date=date(2021, 1, 18)), conv.convert(amount_to, currency,
                                                                                                'USD',
                                                                                                date=date(2021, 1, 18))
    except ValueError:
        if currency in missed_currencies.keys():
            return amount_from * missed_currencies[currency], amount_to * missed_currencies[currency]
        else:
            print("Unknown currency to convert:", currency)


def transform_USD_to_bigmac(salary_from, salary_to, country):
    return salary_from / bm[country], salary_to / bm[country]


def parse_salary(current_salary, country):
    # return month_from_USD, month_to_USD, month_from_bm, month_to_bm
    if pd.isna(current_salary):
        return np.nan, np.nan, np.nan, np.nan
    print('current_salary init', current_salary)
    period = ''
    currency = ''
    symbol = ''
    salary_from = np.nan
    salart_to = np.nan
    for raw_period in periods.keys():
        if raw_period in current_salary:
            period = periods[raw_period]
            current_salary = current_salary.replace(raw_period, '')
    currency, symbol = get_currency(current_salary)
    current_salary = current_salary.replace(symbol, '')
    current_salary = current_salary.replace('万', '0000')
    current_salary = current_salary.replace('収', '')
    current_salary = current_salary.replace('월급', '')
    current_salary = current_salary.replace('Up to', '')
    current_salary = current_salary.replace('From', '')
    current_salary = current_salary.replace('{', '')
    current_salary = current_salary.replace('}', '')
    current_salary = current_salary.replace('\n', '')
    print('current_salary after transforms', current_salary)
    if '-' in current_salary:
        salary_from = current_salary.split('-')[0]
        salary_to = current_salary.split('-')[-1]
    elif '~' in current_salary:
        salary_from = current_salary.split('~')[0]
        salary_to = current_salary.split('~')[-1]
    else:
        salary_from = current_salary
        salary_to = salary_from
    salary_from = salary_to_int(salary_from, period, country)
    salary_to = salary_to_int(salary_to, period, country)
    if period != 'month':
        salary_from, salary_to = calculate_month_salary(salary_from, salary_to, period)
        print("month", salary_from, salary_to)
    if currency != 'USD':
        salary_from, salary_to = convert_salary(salary_from, salary_to, currency)
        print('usd',salary_from, salary_to)
    salary_from_bm, salary_to_bm = transform_USD_to_bigmac(salary_from,salary_to, country)
    return salary_from, salary_to, salary_from_bm, salary_to_bm


def get_sheetname(date):
    name = 'IND'
    month = date.month
    if month in [1,2,3]:
        name += '-Q1'
    elif month in [4,5,6]:
        name += '-Q2'
    elif month in [7,8,9]:
        name += '-Q3'
    elif month in [10, 11, 12]:
        name += '-Q4'
    return name + str(date.year)
