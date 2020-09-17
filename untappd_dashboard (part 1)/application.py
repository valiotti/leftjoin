import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd
from get_ratio_scatter_plot import get_plot
from get_russian_map import get_map
from clickhouse_driver import Client
from dash.dependencies import Input, Output

standard_BS = dbc.themes.BOOTSTRAP

app = dash.Dash(__name__, external_stylesheets=[standard_BS])

app.scripts.config.serve_locally = True
app.css.config.serve_locally = True

client = Client(host='54.227.137.142',
                user='default',
                password='',
                port='9000',
                database='untappd')

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
                                                             'text-transform': 'uppercase'}),
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
                                                                 }),
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
    style={'height': '32.7rem', 'background-color': colors[3]}
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



app.layout =  html.Div([

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
                            html.Hr(), #  dividing line

                            dbc.Row(
                                [
                                    dbc.Col(controls, width={"size": 4,
                                                             "order": 'first',
                                                             "offset": 0},

                                    ),

                                    dbc.Col(dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.H6("Отношение количества отзывов к средней оценке пивоварни",
                                                                    className="card-title",
                                                                    style={'text-transform': 'uppercase'}), # Card Title
                                                            dcc.Graph(id='ratio-scatter-plot'),
                                                        ],
                                                    ),
                                                ],
                                                style={'background-color': colors[2], 'text-align':'center'}
                                             ),
                                    md=8),
                                ],
                                align="start",
                                justify='center',
                            ),
                            html.Br(),
                            html.H5("Заведения и регионы", style={'text-align':'center',
                                                                  'text-transform': 'uppercase'}),
                            html.Hr(), # dividing line
                            # MAP
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dbc.Card(
                                            [
                                                dbc.CardBody(
                                                    [
                                                        html.H6("Средний рейтинг пива по регионам",
                                                                className="card-title",
                                                                style={'text-transform': 'uppercase'},
                                                        ),  # Card Title
                                                        dcc.Graph(figure=get_map())
                                                    ],
                                                ),
                                            ],
                                        style={'background-color': colors[2], 'text-align': 'center'}
                                        ),
                                md=12),
                                ]
                            ),

                            html.Br(),
                        ],
       ), # main div
      fluid=False, style={'max-width': '1300px'}# CONTAINER WIDTH
    )
  ], style={'background-color': colors[1], 'font-family': 'Proxima Nova Bold'}
)

# CALLBACKS

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

application = app.server

if __name__ == '__main__':
    application.run(debug=True, port=8000)







