from clickhouse_driver import Client
import pandas as pd
import plotly.graph_objects as go
import json

client = Client(host='54.227.137.142',
                user='default',
                password='',
                port='9000',
                database='untappd')


with open('russia.json') as russian_geojson:
    counties = json.load(russian_geojson)


def get_map():
    venues_with_avg_rating = client.execute('''
    SELECT region_id, region_name, sum(rating_score) / count(rating_score) as beer_pure_avg FROM
    (
        SELECT beer_id, region_name, region_id, rating_score
        FROM beer_reviews AS t1
        ANY LEFT JOIN (SELECT venue_id, region_id, region_name FROM venues) AS t2 ON t1.venue_id = t2.venue_id 
        GROUP BY beer_id, venue_id, region_id, region_name, rating_score
    )
    GROUP BY region_name, region_id
    ORDER BY region_id
    ''')

    venues_df = pd.DataFrame(venues_with_avg_rating, columns=['region_id', 'region_name', 'beer_pure_avg'])

    fig = go.Figure(go.Choroplethmapbox(geojson=counties,
                                        locations=venues_df['region_id'],
                                        z=venues_df['beer_pure_avg'],
                                        text=venues_df['region_name'],
                                        colorscale=[[0, 'rgb(212, 50, 44)'],
                                                    [0.5, 'rgb(249, 247, 174)'],
                                                    [1, 'rgb(34, 150, 79)']],
                                        colorbar_thickness=20,
                                        hovertemplate='<b>%{text}</b>' + '<br>' +
                                                      'Average beer rating: %{z}' +
                                                      '<extra></extra>',
                                        hoverinfo='text, z'
                                        ))

    fig.update_layout(mapbox_style="carto-positron",
                      mapbox_zoom=1, mapbox_center={"lat": 66, "lon": 94},
                      margin={"r": 0, "t": 0, "l": 0, "b": 0},
                      transition_duration=500,
                      paper_bgcolor='#f8f3e3',
                      font={'family': 'Proxima Nova Regular'},
                      )
    fig.update_traces(marker_line_width=0)


    return fig
