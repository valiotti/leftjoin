import dash_table
import pandas as pd
# from get_data_bars import data_bars
# from reload_data import get_data_best_sorts
import dash_bootstrap_components as dbc
from clickhouse_driver import Client
import requests
import numpy as np
from googletrans import Translator

translator = Translator()

client = Client(host='',
                user='',
                password='',
                port='',
                database='')

city_names = {
    'Moskva': 'Москва',
    'Moscow': 'Москва',
    'СПБ': 'Санкт-Петербург',
    'Saint Petersburg': 'Санкт-Петербург',
    'St Petersburg': 'Санкт-Петербург',
    'Nizhnij Novgorod': 'Нижний Новгород',
    'Tula': 'Тула',
    'Nizhniy Novgorod': 'Нижний Новгород',
}

all_cities_dict_en = {'Москва': 'Moscow',
                'Сергиев Посад': 'Sergiev Posad',
                'Санкт-Петербург': 'Saint Petersburg',
                'Владимир': 'Vladimir',
                'Красная Пахра': 'Red Pakhra',
                'Воронеж': 'Voronezh',
                'Екатеринбург': 'Yekaterinburg',
                'Ярославль': 'Yaroslavl',
                'Казань': 'Kazan',
                'Ростов-на-Дону': 'Rostov-on-Don',
                'Краснодар': 'Krasnodar',
                'Тула': 'Tula',
                'Курск': 'Kursk',
                'Пермь': 'Perm',
                'Нижний Новгород': 'Nizhnij Novgorod',
                }


def get_top_russian_breweries_table(venue_city, checkins_n=250):
    if venue_city == None: # TOP RUSSIAN BREWERIES -> CLICKHOUSE
        selected_df = get_top_russian_breweries(checkins_n)
    else: # BY CITY -> LOCAL
        print(f'GETTING TOP RUSSIAN BREWERIES TABLE FOR {venue_city}')
        ru_city = venue_city
        # if ru_city == 'Санкт-Петербург':
        #     en_city = 'Saint Petersburg'
        # elif ru_city == 'Нижний Новгород':
        #     en_city = 'Nizhnij Novgorod'
        # elif ru_city == 'Пермь':
        #     en_city = 'Perm'
        # else:
        #     en_city = translator.translate(ru_city, dest='en').text
        en_city = all_cities_dict_en[ru_city]
        # best_city_breweries
        df = pd.read_csv(f'data/cities/{en_city}.csv')      # EN
        df = df.loc[df['ЧЕКИНОВ'] >= checkins_n]

        # # MAPPING & CLEANINF DUPLICATES
        df['ГОРОД'] = df['ГОРОД'].map(lambda x: city_names[x] if (x in city_names) else x) # MAPPING

        # df = pd.read_csv(f'data/cities/{venue_city}.csv') # RU
        df.drop_duplicates(subset=['НАЗВАНИЕ', 'ГОРОД'], keep='first', #REMOVING DUPLICATES # lastly
                                    inplace=True)  # keep 1st duplicate row in a df

        df.insert(0, 'МЕСТО',
                           list('🏆 ' + str(i) if i in [1, 2, 3] else str(i) for i in range(1, len(df) + 1)))

        selected_df = df.head(10)
        # print('selection done!')



    table = dbc.Table.from_dataframe(selected_df, striped=False,
                                     bordered=False, hover=True,
                                     size='sm',
                                     style={'background-color': '#ffffff',
                                            'font-family': 'Proxima Nova Regular',
                                            'text-align':'center',
                                            'fontSize': '12px'},
                                     # className='table table-responsive borderless',
                                     className='table borderless'

                                     )

    return table


# ALL-RUSSIAN BREWERIES
def get_top_russian_breweries(checkins_n=250):
    print('GET_TOP_RUSSIAN_BREWERIES')
    top_n_brewery_today = client.execute(f'''
       SELECT  rt.brewery_id,
               rt.brewery_name, 
               beer_pure_average_mult_count/count_for_that_brewery as avg_rating, 
               count_for_that_brewery as checkins FROM (
       SELECT            
               brewery_id,
               dictGet('breweries', 'brewery_name', toUInt64(brewery_id)) as brewery_name,
               sum(rating_score) AS beer_pure_average_mult_count,
               count(rating_score) AS count_for_that_brewery
           FROM beer_reviews t1
           ANY LEFT JOIN venues AS t2 ON t1.venue_id = t2.venue_id
           WHERE isNotNull(venue_id) AND (created_at >= (today() - 30)) AND (venue_country = 'Россия')  
           GROUP BY            
               brewery_id,
               brewery_name) rt
       WHERE (checkins>={checkins_n})
       ORDER BY avg_rating DESC 
       LIMIT 10
       '''
    )
    # top_n_brewery_today_df = pd.DataFrame(top_breweries_today, columns=['brewery_id', 'brewery_name', 'avg_rating', 'checkins'])
    # top_n_brewery_today_df

    top_n_brewery_n_days = client.execute(f'''
       SELECT  rt.brewery_id,
               rt.brewery_name, 
               beer_pure_average_mult_count/count_for_that_brewery as avg_rating, 
               count_for_that_brewery as checkins FROM (
       SELECT            
               brewery_id,
               dictGet('breweries', 'brewery_name', toUInt64(brewery_id)) as brewery_name,
               sum(rating_score) AS beer_pure_average_mult_count,
               count(rating_score) AS count_for_that_brewery
           FROM beer_reviews t1
           ANY LEFT JOIN venues AS t2 ON t1.venue_id = t2.venue_id
           WHERE isNotNull(venue_id) AND (created_at >= (today() - 60) AND created_at <= (today() - 30)) AND (venue_country = 'Россия') 
           GROUP BY            
               brewery_id,
               brewery_name) rt
       WHERE (checkins>={checkins_n})
       ORDER BY avg_rating DESC 
       LIMIT 10
       '''
    )

    # top_n_brewery_n_days = pd.DataFrame(top_n_brewery_n_days, columns=['brewery_id', 'brewery_name', 'avg_rating', 'checkins'])
    # top_n_brewery_n_days

    top_n = len(top_n_brewery_today)
    column_names = ['brewery_id', 'brewery_name', 'avg_rating', 'checkins']

    top_n_brewery_today_df = pd.DataFrame(top_n_brewery_today, columns=column_names).replace(np.nan, 0)
    top_n_brewery_today_df['brewery_pure_average'] = round(top_n_brewery_today_df.avg_rating, 2)
    top_n_brewery_today_df['brewery_rank'] = list(range(1, top_n + 1))
    # top_n_brewery_today_df #6 columns

    top_n_brewery_n_days = pd.DataFrame(top_n_brewery_n_days, columns=column_names).replace(np.nan, 0)
    top_n_brewery_n_days['brewery_pure_average'] = round(top_n_brewery_n_days.avg_rating, 2)
    top_n_brewery_n_days['brewery_rank'] = list(range(1, len(top_n_brewery_n_days) + 1))
    # top_n_brewery_n_days #6 columns

    rank_was_list = []
    for brewery_id in top_n_brewery_today_df.brewery_id:
        try:
            rank_was_list.append(
                top_n_brewery_n_days[top_n_brewery_n_days.brewery_id == brewery_id].brewery_rank.item())
        except ValueError:
            rank_was_list.append('–')
    top_n_brewery_today_df['rank_was'] = rank_was_list
    # top_n_brewery_today_df # 7 columns

    text_color_list = []
    text_color = []
    cols_to_show = ['Место в рейтинге', 'Название пивоварни', 'Средний рейтинг пивоварни', 'Изменение места в рейтинге']
    diff_rank_list = []
    n = len(top_n_brewery_today_df)
    for rank_was, rank_now in zip(top_n_brewery_today_df['rank_was'], top_n_brewery_today_df['brewery_rank']):
        if rank_was != '–':
            difference = rank_was - rank_now
            if difference > 0:
                diff_rank_list.append(f'↑ +{difference}')
            elif difference < 0:
                diff_rank_list.append(f'↓ {difference}')
            else:
                diff_rank_list.append('–')
        else:
            diff_rank_list.append(rank_was)
    for col in cols_to_show:
        if col != 'Изменение места в рейтинге':
            text_color.append(['black'] * n)
        else:
            text_color.append(text_color_list)


    df = top_n_brewery_today_df[['brewery_name', 'avg_rating', 'checkins']].round(2)
    df.insert(2, 'Изменение', diff_rank_list)
    df.columns = ['НАЗВАНИЕ', 'РЕЙТИНГ', 'ИЗМЕНЕНИЕ', 'ЧЕКИНОВ']
    df.insert(0, 'МЕСТО',
              list('🏆 ' + str(i) if i in [1, 2, 3] else str(i) for i in range(1, len(df) + 1)))


    return df



def get_best_beer_sorts_table_1(checkins_n=250):
    df = pd.read_csv(f'data/top_best_beers.csv')
    selected_df = df.loc[df['ЧЕКИНОВ'] >= checkins_n].head(10)
    selected_df.insert(0, 'МЕСТО',
                       list('🏆 ' + str(i) if i in [1, 2, 3] else str(i) for i in range(1, len(selected_df) + 1)))

    table = dbc.Table.from_dataframe(selected_df, striped=False,
                                     bordered=False, hover=True,
                                     size='sm',
                                     style={'background-color': '#ffffff',
                                           'font-family': 'Proxima Nova Regular',
                                           'text-align': 'center',
                                           'fontSize': '12px'},
                                     # className='table table-responsive borderless',

                                     className='table borderless'
                                     )
    return table


# def get_best_beer_sorts_table_1(sort_list=None):
#     if sort_list is None:
#         df_table_top_beers = pd.read_csv('data/top_best_beers.csv')
#     else:
#         df_table_top_beers = get_data_best_sorts(sort_list)
#
#     table = dbc.Table.from_dataframe(df_table_top_beers, striped=False,
#                                      bordered=False, hover=True,
#                                      size='sm',
#                                      style={'background-color': '#ffffff',
#                                            'font-family': 'Proxima Nova Regular',
#                                            'text-align': 'center',
#                                            'fontSize': '12px'},
#                                      className='table borderless'
#
#                                      )
#
#     return table


def get_worst_beer_sorts_table_1(checkins_n=250):
    df = pd.read_csv(f'data/top_worst_beers.csv')
    selected_df = df.loc[df['ЧЕКИНОВ'] >= checkins_n].head(10)
    selected_df.insert(0, 'МЕСТО',
                       list('🏆 ' + str(i) if i in [1, 2, 3] else str(i) for i in range(1, len(selected_df) + 1)))

    table = dbc.Table.from_dataframe(selected_df, striped=False,
                                     bordered=False, hover=True,
                                     size='sm',
                                     style={'background-color': '#ffffff',
                                           'font-family': 'Proxima Nova Regular',
                                           'text-align': 'center',
                                           'fontSize': '12px'},

                                     # className='table table-responsive borderless', # only horizontally
                                     className='table borderless'
                                    )

    return table



# SCHEDULING UPDATES
# UPDATING TABLES
def update_best_breweries(venue_city):
    ru_city = venue_city
    # if ru_city == 'Санкт-Петербург':
    #     en_city = 'Saint Petersburg'
    # elif ru_city == 'Нижний Новгород':
    #     en_city = 'Nizhnij Novgorod'
    # elif ru_city == 'Пермь':
    #     en_city = 'Perm'
    # else:
    #     en_city = translator.translate(ru_city, dest='en').text
    en_city = all_cities_dict_en[ru_city]
    print(ru_city, en_city)
    top_n_brewery_today = client.execute(f'''
    SELECT  rt.brewery_id,
            rt.venue_city, rt.brewery_name, 
            beer_pure_average_mult_count/count_for_that_brewery as avg_rating, 
            count_for_that_brewery as checkins FROM (
    SELECT 
            venue_city,
            brewery_id,
            dictGet('breweries', 'brewery_name', toUInt64(brewery_id)) as brewery_name,
            sum(rating_score) AS beer_pure_average_mult_count,
            count(rating_score) AS count_for_that_brewery
        FROM beer_reviews t1
        ANY LEFT JOIN venues AS t2 ON t1.venue_id = t2.venue_id
        WHERE isNotNull(venue_id) AND (created_at >= (today() - 30)) AND (venue_country = 'Россия') 
        GROUP BY 
            venue_city,
            brewery_id,
            brewery_name) rt
    WHERE (rt.venue_city='{ru_city}' OR rt.venue_city='{en_city}') 
    ORDER BY avg_rating DESC    
    '''
                                         )

    # top_n_brewery_today_df = pd.DataFrame(top_n_brewery_today, columns=['brewery_id', 'venue_city', 'brewery_name', 'avg_rating', 'checkins'])
    # top_n_brewery_today_df

    top_n_brewery_n_days = client.execute(f'''
    SELECT  
            rt.brewery_id,
            rt.venue_city, rt.brewery_name, 
            beer_pure_average_mult_count/count_for_that_brewery as avg_rating, 
            count_for_that_brewery as checkins FROM (
    SELECT 
            venue_city,
            brewery_id,
            dictGet('breweries', 'brewery_name', toUInt64(brewery_id)) as brewery_name,
            sum(rating_score) AS beer_pure_average_mult_count,
            count(rating_score) AS count_for_that_brewery
        FROM beer_reviews t1
        ANY LEFT JOIN venues AS t2 ON t1.venue_id = t2.venue_id
        WHERE isNotNull(venue_id) AND (created_at >= (today() - 60) AND created_at <= (today() - 30)) AND (venue_country = 'Россия') 
        GROUP BY 
            venue_city,
            brewery_id,
            brewery_name) rt
    WHERE (rt.venue_city='{ru_city}' OR rt.venue_city='{en_city}')
    ORDER BY avg_rating DESC 
    '''
                                          )
    # top_n_brewery_n_days_df = pd.DataFrame(top_n_brewery_n_days, columns=['brewery_id', 'venue_city', 'brewery_name', 'avg_rating', 'checkins'])
    # top_n_brewery_n_days_df

    top_n = len(top_n_brewery_today)
    column_names = ['brewery_id', 'venue_city', 'brewery_name', 'avg_rating', 'checkins']

    top_n_brewery_today_df = pd.DataFrame(top_n_brewery_today, columns=column_names).replace(np.nan, 0)
    top_n_brewery_today_df['brewery_pure_average'] = round(top_n_brewery_today_df.avg_rating, 2)
    top_n_brewery_today_df['brewery_rank'] = list(range(1, top_n + 1))
    # top_n_brewery_today_df
    top_n_brewery_n_days = pd.DataFrame(top_n_brewery_n_days, columns=column_names).replace(np.nan, 0)
    top_n_brewery_n_days['brewery_pure_average'] = round(top_n_brewery_n_days.avg_rating, 2)
    top_n_brewery_n_days['brewery_rank'] = list(range(1, len(top_n_brewery_n_days) + 1))
    # top_n_brewery_n_days

    rank_was_list = []
    for brewery_id in top_n_brewery_today_df.brewery_id:
        try:
            rank_was_list.append(
                top_n_brewery_n_days[top_n_brewery_n_days.brewery_id == brewery_id].brewery_rank.item())
        except ValueError:
            rank_was_list.append('–')
    top_n_brewery_today_df['rank_was'] = rank_was_list
    # top_n_brewery_today_df

    text_color_list = []
    text_color = []
    cols_to_show = ['Место в рейтинге', 'Название пивоварни', 'Средний рейтинг пивоварни', 'Изменение места в рейтинге']
    diff_rank_list = []
    n = len(top_n_brewery_today_df)
    for rank_was, rank_now in zip(top_n_brewery_today_df['rank_was'], top_n_brewery_today_df['brewery_rank']):
        if rank_was != '–':
            difference = rank_was - rank_now
            if difference > 0:
                diff_rank_list.append(f'↑ +{difference}')
            elif difference < 0:
                diff_rank_list.append(f'↓ {difference}')
            else:
                diff_rank_list.append('–')
        else:
            diff_rank_list.append(rank_was)
    for col in cols_to_show:
        if col != 'Изменение места в рейтинге':
            text_color.append(['black'] * n)
        else:
            text_color.append(text_color_list)

    df = top_n_brewery_today_df[['brewery_name', 'venue_city', 'avg_rating', 'checkins']].round(2)
    df.insert(3, 'Изменение', diff_rank_list)
    df.columns = ['НАЗВАНИЕ', 'ГОРОД', 'РЕЙТИНГ', 'ИЗМЕНЕНИЕ', 'ЧЕКИНОВ']
    df.to_csv(f'data/cities/{en_city}.csv', index=False)  # saving DF
    print(f'{en_city}.csv updated!')


# BEERS
flatten = lambda l: [item for sublist in l for item in sublist]

sorts = flatten(client.execute('''
SELECT beer_style FROM beers
GROUP BY beer_style
'''))

def update_best_beers():
    sort_list = sorts
    top_n_beers_today = client.execute(f'''
        SELECT
            beer_name,
            beer_id,
            brewery_id,
            count(rating_score) AS count_rating_score,
            sum(rating_score) / count(rating_score) AS beer_pure_avg,
            dictGet('breweries', 'brewery_name', toUInt64(brewery_id)) as brewery_name
        FROM beer_reviews AS t1
        ANY LEFT JOIN (SELECT beer_id, beer_name, beer_style FROM beers) AS t2 ON t1.beer_id = t2.beer_id
        WHERE created_at >= today() - 30 AND beer_style in {str(sort_list)}
        GROUP BY beer_id, brewery_id, beer_style, beer_name
        ORDER BY beer_pure_avg DESC
                    ''')

    top_n_beers_n_days = client.execute(f'''
        SELECT
            beer_name,
            beer_id,
            brewery_id,
            count(rating_score) AS count_rating_score,
            sum(rating_score) / count(rating_score) AS beer_pure_avg,
            dictGet('breweries', 'brewery_name', toUInt64(brewery_id)) as brewery_name
        FROM beer_reviews AS t1
        ANY LEFT JOIN (SELECT beer_id, beer_name, beer_style FROM beers) AS t2 ON t1.beer_id = t2.beer_id
        WHERE created_at >= today() - 60 AND created_at <= today() - 30 AND beer_style in {str(sort_list)}
        GROUP BY beer_id, brewery_id, beer_style, beer_name
        ORDER BY beer_pure_avg DESC       
                    ''')
    top_n_best_beer_today_df = pd.DataFrame(top_n_beers_today,
                                            columns=['beer_name', 'beer_id', 'brewery_id', 'count_rating_score',
                                                     'beer_pure_average',
                                                     'brewery_name'])
    top_n_best_beer_today_df.beer_pure_average = round(top_n_best_beer_today_df.beer_pure_average, 2)
    top_n_best_beer_today_df.insert(0, 'beer_rank', list(range(1, len(top_n_best_beer_today_df) + 1)))
    # top_n_best_beer_today_df

    top_n_beer_n_days_df = pd.DataFrame(top_n_beers_n_days,
                                        columns=['beer_name', 'beer_id', 'brewery_id', 'count_rating_score',
                                                 'beer_pure_average',
                                                 'brewery_name'])
    top_n_beer_n_days_df.beer_pure_average = round(top_n_beer_n_days_df.beer_pure_average, 2)
    top_n_beer_n_days_df.insert(0, 'beer_rank', list(range(1, len(top_n_beer_n_days_df) + 1)))
    # top_n_beers_n_days_df

    rank_was_list = []
    for beer_id in top_n_best_beer_today_df.beer_id:
        try:
            rank_was_list.append(
                top_n_beer_n_days_df[top_n_beer_n_days_df.beer_id == beer_id].beer_rank.item())
        except ValueError:
            rank_was_list.append('–')
    top_n_best_beer_today_df['rank_was'] = rank_was_list
    # top_n_best_beer_today_df

    text_color_list = []
    text_color = []
    cols_to_show = ['Место в рейтинге', 'Название пивоварни', 'Средний рейтинг пивоварни', 'Изменение места в рейтинге']
    diff_rank_list = []
    n = len(top_n_best_beer_today_df)
    for rank_was, rank_now in zip(top_n_best_beer_today_df['rank_was'], top_n_best_beer_today_df['beer_rank']):
        if rank_was != '–':
            difference = rank_was - rank_now
            if difference > 0:
                diff_rank_list.append(f'↑ +{difference}')
            elif difference < 0:
                diff_rank_list.append(f'↓ -{difference}')
            else:
                diff_rank_list.append('–')
        else:
            diff_rank_list.append(rank_was)
    for col in cols_to_show:
        if col != 'Изменение места в рейтинге':
            text_color.append(['black'] * n)
        else:
            text_color.append(text_color_list)

    # top_n_best_beer_today_df

    df = top_n_best_beer_today_df[['beer_name', 'brewery_name', 'beer_pure_average', 'count_rating_score']]
    df.insert(3, 'Изменение', diff_rank_list)
    df.columns = ['СОРТ ПИВА', 'ПИВОВАРНЯ', 'РЕЙТИНГ', 'ИЗМЕНЕНИЕ', 'ЧЕКИНОВ']
    df.to_csv(f'data/top_best_beers.csv', index=False)  # saving DF
    print(f'top_best_beers.csv updated!')

def update_worst_beers():
    worst_n_beers_today = client.execute(f'''
    SELECT   
       beer_id,
       brewery_id,
       count(rating_score) AS count_rating_score,
       sum(rating_score) / count(rating_score) AS beer_pure_avg,
       dictGet('breweries', 'brewery_name', toUInt64(brewery_id)) as brewery_name
    FROM beer_reviews
    WHERE created_at >= today() - 30
    GROUP BY beer_id, brewery_id
    ORDER BY beer_pure_avg ASC
    ''')

    worst_n_beers_n_days = client.execute(f'''
    SELECT  
       beer_id,
       brewery_id,
       count(rating_score) AS count_rating_score,
       sum(rating_score) / count(rating_score) AS beer_pure_avg,
       dictGet('breweries', 'brewery_name', toUInt64(brewery_id)) as brewery_name
    FROM beer_reviews
    WHERE created_at >= today() - 60 AND created_at <= today() - 30
    GROUP BY beer_id, brewery_id
    ORDER BY beer_pure_avg ASC
    ''')

    worst_n_beers_today_df = pd.DataFrame(worst_n_beers_today,
                                          columns=['beer_id', 'brewery_id', 'count_rating_score', 'beer_pure_average',
                                                   'brewery_name'])
    worst_n_beers_today_df.beer_pure_average = round(worst_n_beers_today_df.beer_pure_average, 2)
    worst_n_beers_today_df.insert(0, 'beer_rank', list(range(1, len(worst_n_beers_today_df) + 1)))
    # worst_n_best_beer_today_df

    worst_n_beers_n_days_df = pd.DataFrame(worst_n_beers_n_days,
                                           columns=['beer_id', 'brewery_id', 'count_rating_score', 'beer_pure_average',
                                                    'brewery_name'])
    worst_n_beers_n_days_df.beer_pure_average = round(worst_n_beers_n_days_df.beer_pure_average, 2)
    worst_n_beers_n_days_df.insert(0, 'beer_rank', list(range(1, len(worst_n_beers_n_days_df) + 1)))
    # worst_n_beers_n_days_df

    # ADDING BEER NAME
    beer_ids_lst = worst_n_beers_today_df['beer_id'].values.tolist()
    beer_names = client.execute(f'SELECT beer_name, beer_id FROM beers WHERE beer_id in {str(beer_ids_lst)}')
    beer_names_df = pd.DataFrame(beer_names, columns=['beer_name', 'beer_id'])

    # beer_names_df   

    # CLEANING DF
    worst_n_beers_today_df = pd.merge(left=worst_n_beers_today_df, right=beer_names_df, left_on='beer_id',
                                      right_on='beer_id')
    worst_n_beers_today_df.drop_duplicates(subset="beer_id",
                                           keep=False, inplace=True)

    rank_was_list = []
    for beer_id in worst_n_beers_today_df.beer_id:
        try:
            rank_was_list.append(
                worst_n_beers_n_days_df[worst_n_beers_n_days_df.beer_id == beer_id].beer_rank.item())
        except ValueError:
            rank_was_list.append('–')
    worst_n_beers_today_df['rank_was'] = rank_was_list
    # worst_n_beers_today_df

    text_color_list = []
    text_color = []
    cols_to_show = ['Место в рейтинге', 'Название пивоварни', 'Средний рейтинг пивоварни', 'Изменение места в рейтинге']
    diff_rank_list = []
    n = len(worst_n_beers_today_df)
    for rank_was, rank_now in zip(worst_n_beers_today_df['rank_was'], worst_n_beers_today_df['beer_rank']):
        if rank_was != '–':
            difference = rank_was - rank_now
            if difference > 0:
                diff_rank_list.append(f'↑ +{difference}')
            elif difference < 0:
                diff_rank_list.append(f'↓ -{difference}')
            else:
                diff_rank_list.append('–')
        else:
            diff_rank_list.append(rank_was)
    for col in cols_to_show:
        if col != 'Изменение места в рейтинге':
            text_color.append(['black'] * n)
        else:
            text_color.append(text_color_list)

    df = worst_n_beers_today_df[['beer_name', 'brewery_name', 'beer_pure_average', 'count_rating_score']]
    df.insert(3, 'Изменение', diff_rank_list)
    df.columns = ['СОРТ ПИВА', 'ПИВОВАРНЯ', 'РЕЙТИНГ', 'ИЗМЕНЕНИЕ', 'ЧЕКИНОВ']
    df.to_csv(f'data/top_worst_beers.csv', index=False)  # saving DF
    print(f'top_worst_beers.csv updated!')










# UPDATE BEFORE RERUN
# all_cities = sorted(['Москва', 'Сергиев Посад', 'Санкт-Петербург', 'Владимир',
#               'Красная Пахра', 'Воронеж', 'Екатеринбург', 'Ярославль', 'Казань',
#               'Ростов-на-Дону', 'Краснодар', 'Тула', 'Курск', 'Пермь', 'Нижний Новгород'])

# for city in all_cities:
#     update_best_breweries(city)
# print(f'Breweries Table updated at {current_time}')
# Updating beers
# update_best_beers()
# update_worst_beers()





















