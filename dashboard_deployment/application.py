import dash
import dash_core_components as dcc
import dash_html_components as html
from get_plots import get_scatter_plot
import pandas as pd
from clickhouse_driver import Client

client = Client(host='ec1-2-34-567-89.us-east-2.compute.amazonaws.com', user='default', password='', port='9000', database='default')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.Div([
       html.Div([
            html.Div([
                html.H6('Временной период, дней'),
                dcc.Slider(
                    id='slider-day1',
                    min=0,
                    max=100,
                    step=1,
                    value=100,
                    marks={i: str(i) for i in range(0, 101, 10)})
            ], style={'width': '49%', 'display': 'inline-block', 'font-family':'PT Sans'}),
            html.Div([
                html.H6('Количество пивоварен в топе'),
                dcc.Slider(
                    id='slider-top1',
                    min=0,
                    max=200,
                    step=50,
                    value=200,
                    marks={i: str(i) for i in range(0, 201, 50)})
            ], style={'width': '49%', 'display': 'inline-block', 'font-family': 'PT Sans'})],
        style={
        'backgroundColor': 'rgb(255, 255, 255)',
        'padding': '10px 5px',
        'font-family': 'PT Sans'
        }),
        html.Div([
           dcc.Graph(id='fig1')
       ]),
    ])
])


@app.callback(
   dash.dependencies.Output('fig1', 'figure'),
   [dash.dependencies.Input('slider-day1', 'value'),
    dash.dependencies.Input('slider-top1', 'value')])
def output_fig(n_days=100, top_n=200):
    df = pd.read_csv('preloaded_data.csv')
    if n_days == 100 and top_n == 200:
        fig = get_scatter_plot(n_days, top_n, df)
    else:
        fig = get_scatter_plot(n_days, top_n)
    return fig


application = app.server

if __name__ == '__main__':
    application.run(debug=True, port=8080)

