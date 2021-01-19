import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd
from get_ratio_scatter_plot import get_plot
from get_russian_map import get_map
from clickhouse_driver import Client
from dash.dependencies import Input, Output
# Tables
from get_tables import get_top_russian_breweries_table, get_worst_beer_sorts_table_1, get_best_beer_sorts_table_1
# Scheduling
from apscheduler.schedulers.background import BackgroundScheduler


standard_BS = dbc.themes.BOOTSTRAP
app = dash.Dash(__name__, external_stylesheets=[standard_BS])

app.scripts.config.serve_locally = True
app.css.config.serve_locally = True

client = Client(host='',
                user='',
                password='',
                port='',
                database='')


all_cities = sorted(['Москва', 'Сергиев Посад', 'Санкт-Петербург', 'Владимир',
              'Красная Пахра', 'Воронеж', 'Екатеринбург', 'Ярославль', 'Казань',
              'Ростов-на-Дону', 'Краснодар', 'Тула', 'Курск', 'Пермь', 'Нижний Новгород'])


colors = ['#ffcc00', # золотой header
          '#f5f2e8', # background main
          '#f8f3e3', # card main
          '#ffffff', # белый
          ]

slider_day_values = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
slider_top_breweries_values = [5, 25, 50, 75, 100, 125, 150, 175, 200]

controls = dbc.Card(
    [
       dbc.CardBody(
           [
               dbc.FormGroup(
                    [
                        dbc.Label("Временной период", style={'text-align': 'center', 'font-size': '100%',
                                                             'font-family': 'Proxima Nova Regular',
                                                             'text-transform': 'uppercase'}
                        ),
                        dcc.Slider(
                            id='slider-day',
                            min=1,
                            max=100,
                            step=10,
                            value=100,
                            marks={i: i for i in slider_day_values}
                        ),
                    ], style={'text-align': 'center'}
               ),
               dbc.FormGroup(
                    [
                        dbc.Label("Количество пивоварен", style={'text-align': 'center', 'font-size': '100%',
                                                                 'font-family': 'Proxima Nova Regular',
                                                                 'text-transform': 'uppercase',
                                                                 }
                        ),
                        dcc.Slider(
                            id='slider-top-breweries',
                            min=5,
                            max=200,
                            step=5,
                            value=200,
                            marks={i: i for i in slider_top_breweries_values}
                        ),
                    ], style={'text-align': 'center'}
               ),
           ],
       )
    ],
    style={'height': '25rem', 'background-color': colors[3]}
)

logo = html.Img(src=app.get_asset_url('logo.png'),
                        style={'width': "128px", 'height': "128px",
                        }, className='inline-image')

header = html.H3("Статистика российских пивоварен в Untappd", style={'text-transform': "uppercase"})


logo_and_header = dbc.FormGroup(
        [
            logo,
            html.Div(
                [
                    header
                ],
                className="p-5"
            )
        ],
        className='form-row',
)


# Scatter&Controls
scatter_and_controls = dbc.Card(
    [
        dbc.CardBody(
            [
                # html.P(id="scatter_header"),
                html.H6(id="scatter_header",
                        className="card-title",
                        style={'text-transform': 'uppercase'}),
                dcc.Graph(id='ratio-scatter-plot'),
                html.Br(),
                dbc.FormGroup(
                    [
                        dbc.FormGroup(
                                [
                                dbc.Label("Временной период", style={'text-align': 'center', 'font-size': '100%'}),
                                dcc.Slider(
                                    id='slider-day',
                                    min=1,
                                    max=100,
                                    step=10,
                                    value=100,
                                    marks={i: i for i in slider_day_values}
                                )
                                ],
                                style={'text-align': 'center'}, className='form-group col-md-6'
                        ),
                        dbc.FormGroup(
                                [

                                    dbc.Label("Количество пивоварен", style={'text-align': 'center', 'font-size': '100%'}),
                                    dcc.Slider(
                                        id='slider-top-breweries',
                                        min=5,
                                        max=200,
                                        step=5,
                                        value=200,
                                        marks={i: i for i in slider_top_breweries_values}
                                    ),
                                ],
                                style={'text-align': 'center'}, className='form-group col-md-6'
                        ),
                    ],
                    className='form-row',
                ),
            ],
        ),
    ],
    style={'background-color': colors[2], 'text-align': 'center', 'height': '39rem'} # Card Style
)

russian_map = dbc.Card(
    [
        dbc.CardBody(
            [
                html.H6("Средний рейтинг пива по регионам",
                        className="card-title",
                        style={'text-transform': 'uppercase'},
                ),  # Card Title
                dcc.Graph(figure=get_map(), style={'height': '35rem'}
                          ) # graph size
            ],
        ),
    ],
    style={'background-color': colors[2], 'text-align':'center', 'height': '39rem'} # Card Style
)


charts = dbc.Card(
    [
        dbc.CardHeader(
            dbc.Tabs(
                [
                    dbc.Tab(scatter_and_controls, label="Пивоварни"),
                    dbc.Tab(russian_map, label='Регионы',
                    ),
                ],
            ),
        ),
    ],
    style={'background-color': colors[2]},
),

# CHECKINS SLIDERS
checkins_slider_tab_1 = dbc.CardBody(
                            dbc.FormGroup(
                                [
                                    html.H6('Количество чекинов', style={'text-align': 'center'}),
                                    # dbc.Label("Чекины", style={'text-align': 'center', 'font-size': '100%'}
                                    # ),
                                    dcc.Slider(
                                        id='checkin_n_tab_1',
                                        min=0,
                                        max=250,
                                        step=25,
                                        value=250,  # initial value
                                        loading_state={'is_loading': True},
                                        marks={i: i for i in list(range(0, 251, 25))},
                                        # marks={i: i for i in list(range(10, 51, 5))},
                                    ),
                                ],
                            ),
                            style={'max-height': '80px', # controlling card body height
                                   'padding-top': '25px'
                                   }
                        )

checkins_slider_tab_2 = dbc.CardBody(
                            dbc.FormGroup(
                                [
                                    html.H6('Количество чекинов', style={'text-align': 'center'}),
                                    dcc.Slider(
                                        id='checkin_n_tab_2',
                                        min=0,
                                        max=250,
                                        step=25,
                                        value=250,  # initial value
                                        loading_state={'is_loading': True},
                                        marks={i: i for i in list(range(0, 251, 25))},
                                   ),
                                ],
                            ),
                            style={'max-height': '80px',
                                   'padding-top': '25px'
                                   }  # controlling card body height
                        )

checkins_slider_tab_3 = dbc.CardBody(
                            dbc.FormGroup(
                                [
                                    html.H6('Количество чекинов', style={'text-align': 'center'}),
                                    dcc.Slider(
                                        id='checkin_n_tab_3',
                                        min=0,
                                        max=250,
                                        step=25,
                                        value=250,  # initial value
                                        loading_state={'is_loading': True},
                                        marks={i: i for i in list(range(0, 251, 25))},
                                    ),
                                ],
                            ),
                            style={'max-height': '80px',
                                   'padding-top': '25px'
                                   }  # controlling card body height
                        )
# Tables
tables = dbc.Card(
    [
        dbc.CardHeader(
            dbc.Tabs(
                [
                    dbc.Tab(children=checkins_slider_tab_1, label="Лучшие пивоварни", tab_id="tab-1"),
                    dbc.Tab(children=checkins_slider_tab_2, label="Лучшие сорта пива", tab_id="tab-2"),
                    dbc.Tab(children=checkins_slider_tab_3, label="Худшие сорта пива", tab_id="tab-3"),
                ],
                id="card-tabs-1",
                card=True,
                active_tab="tab-1",
            )
        ),

        html.Div(
            [
                dbc.CardBody(
                            [
                                html.P(id="card-content-1", className="card-text"), # table
                            ],
                ),
            ],
            className='overflow-auto', # PREVENT CARD CONTENT OVERFLOW
        )
    ],
    style={'height': '43.4rem'}
)


top_breweries = dbc.Card(
        [
            dbc.CardBody(
                [
                    # DROPDOWN MENU
                    dbc.FormGroup(
                        [
                            html.H6('Фильтр городов', style={'text-align': 'center'}),
                            dcc.Dropdown(
                                id='city_menu',
                                options=[{'label': i, 'value': i} for i in all_cities],
                                multi=False,
                                placeholder='Выберите город',
                                style={'font-family': 'Proxima Nova Regular'}
                            ),
                        ],
                    ),
                    html.P(id="tab-1-content", className="card-text"),
                ],
            ),
    ],
)

app.layout = html.Div([
                dbc.Container(
                    dbc.Row(
                        [
                            dbc.Col(
                                html.Div(
                                    logo_and_header,
                                ),
                            ),
                        ],
                        style={'max-height': '128px',
                               'color': 'white',
                       }

                    ),
                    className='d-flex justify-content-center',
                    style={'max-width': '100%',
                           'background-color': colors[0]},
                ),
                dbc.Container(
                    html.Div(
                        [
                            html.Br(),
                            html.H5("Пивоварни", style={'text-align':'center',
                                                        'text-transform': 'uppercase'}),
                            html.Hr(), # dividing line

                            dbc.Row(
                                [
                                    dbc.Col(tables, md=5
                                    ),

                                    dbc.Col(charts
                                    ),
                                ],
                                align="start",
                                justify='center',
                            ),
                            html.Br(),
                            html.Br(),
                        ],
       ), # main div
      fluid=False, style={'max-width': '1525px'}# CONTAINER WIDTH
    ) # contatiner 1
  ], style={'background-color': colors[1], 'font-family': 'Proxima Nova Bold'}
)



# CALLBACKS
# Scatter Plot
@app.callback(
    Output('ratio-scatter-plot', 'figure'),
    [Input('slider-day', 'value'),
     Input('slider-top-breweries', 'value'),
     ])
def get_scatter_plots(n_days=100, top_n=200):
    if n_days == 100 and top_n == 200:
        df = pd.read_csv('data/ratio_scatter_plot.csv')
        return get_plot(n_days, top_n, df)
    else:
        return get_plot(n_days, top_n)




# Tables
# Beer
@app.callback(
    Output("card-content-1", "children"), [Input("card-tabs-1", "active_tab"),
                                           Input("checkin_n_tab_2", "value"),
                                           Input('checkin_n_tab_3', 'value'),
                                           ]
)
def tab_content(active_tab, checkin_slider_2, checkin_slider_3):
    print(active_tab, checkin_slider_2)
    if active_tab == "tab-1":
        return top_breweries, html.P('*данные за последние 30 дней', style={'font-size': '12px',
                                                              'text-align': 'right',
                                                              'font-family': 'Proxima Nova Regular'
                                                                            }
                                     ) # small text
    elif active_tab == "tab-2":
        return dbc.Card(dbc.CardBody([get_best_beer_sorts_table_1(checkin_slider_2), html.P('*данные за последние 30 дней', style={'font-size': '12px',
                                                              'text-align': 'right',
                                                              'font-family': 'Proxima Nova Regular'
                                                              }
                                                              ) # small text
        ]))

    elif active_tab == 'tab-3':
        return dbc.Card(dbc.CardBody([get_worst_beer_sorts_table_1(checkin_slider_3), html.P('*данные за последние 30 дней', style={'font-size': '12px',
                                                              'text-align': 'right',
                                                              'font-family': 'Proxima Nova Regular'
                                                              }
                                                              ) # small text
        ]))


# Tab-1 Breweries table
@app.callback(
    Output("tab-1-content", "children"), [Input("city_menu", "value"), # dropdown menu
                                          Input("checkin_n_tab_1", "value")] # slider for tab-1
)
def table_content(city, checkin_n):
    print(city, checkin_n) # if city==None the func returns all-russian breweries rating
    # print(f'GETTING TOP RUSSIAN BREWERIES TABLE FOR {city}')
    return get_top_russian_breweries_table(city, checkin_n)

# Scatter Header
@app.callback(
    Output("scatter_header", "children"), [Input('slider-day', 'value')]
)
def scatter_header(slider_value):
    print(slider_value)
    return f"Отношение количества отзывов к средней оценке пивоварни за последние {slider_value} дней" # children

application = app.server

#-----------------------------------------------------------------------------------------------------------------------
# UPDATING DATA
from get_tables import update_best_breweries, update_best_beers, update_worst_beers
# from datetime import datetime

# Scheduling
scheduler = BackgroundScheduler()
@scheduler.scheduled_job('cron', hour=16, misfire_grace_time=45) # sec
def update_data():
    # current_time = datetime.now().time()
    # print(f'START UPDATING DATA AT {current_time}')
    # Updating breweries table
    for city in all_cities:
        update_best_breweries(city)
    # print(f'Breweries Table updated at {current_time}')
    # Updating beers
    update_best_beers()
    update_worst_beers()

scheduler.start()



#-----------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    application.run(debug=True, port=8020)
    # help(dcc.Loading)














