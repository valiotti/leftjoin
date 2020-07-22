from clickhouse_driver import Client
import plotly.graph_objects as go
import pandas as pd
import numpy as np
client = Client(host='ec2-3-16-148-63.us-east-2.compute.amazonaws.com', user='default', password='', port='9000', database='default')


def get_scatter_plot(n_days, top_n, data=None):
    if data is None:
        brewery_pure_average = client.execute(f'''
        SELECT 
            t1.brewery_id, 
            sum(t1.beer_pure_average_mult_count / t2.count_for_that_brewery) AS brewery_pure_average, 
            t2.count_for_that_brewery, 
            dictGet('breweries', 'brewery_name', toUInt64(t1.brewery_id)), 
            dictGet('breweries', 'beer_count', toUInt64(t1.brewery_id)),
            t3.stats_age_on_service / 365
    
        FROM 
        (
            SELECT 
                beer_id, 
                brewery_id, 
                sum(rating_score) AS beer_pure_average_mult_count
            FROM beer_reviews
            WHERE created_at >= today()-{n_days}
            GROUP BY 
                beer_id, 
                brewery_id
        ) AS t1
        ANY LEFT JOIN 
        (
            SELECT 
                brewery_id, 
                count(rating_score) AS count_for_that_brewery
            FROM beer_reviews
            WHERE created_at >= today()-{n_days}
            GROUP BY brewery_id
        ) AS t2 ON t1.brewery_id = t2.brewery_id
        ANY LEFT JOIN
        (
            SELECT
                brewery_id,
                stats_age_on_service
            FROM brewery_info_new
        ) AS t3 ON t1.brewery_id = t3.brewery_id
        GROUP BY 
            t1.brewery_id, 
            t2.count_for_that_brewery,
            t3.stats_age_on_service
        HAVING t2.count_for_that_brewery >= 150
        ORDER BY brewery_pure_average
        LIMIT {top_n}
        ''')
        scatter_plot_df_with_age = pd.DataFrame(brewery_pure_average, columns=['brewery_id', 'brewery_pure_average', 'rating_count', 'brewery_name', 'beer_count', 'age_on_service'])
    else:
        scatter_plot_df_with_age = data
    brewery_name_and_beer_count = []
    for name, beer_count in zip(scatter_plot_df_with_age.brewery_name, scatter_plot_df_with_age.beer_count):
        brewery_name_and_beer_count.append(f'{name},<br>количество сортов пива: {beer_count}')
    scatter_plot_df_with_age['name_count'] = brewery_name_and_beer_count
    dict_list = list()
    dict_list.append(dict(type="line",
        line=dict(
             color="#666666",
             dash="dot"),
        x0=0,
        y0=np.median(scatter_plot_df_with_age.brewery_pure_average),
        x1=9000,
        y1=np.median(scatter_plot_df_with_age.brewery_pure_average),
        line_width=1,
        layer="below"))
    dict_list.append(dict(type="line",
        line=dict(
             color="#666666",
             dash="dot"),
        x0=np.median(scatter_plot_df_with_age.rating_count),
        y0=0,
        x1=np.median(scatter_plot_df_with_age.rating_count),
        y1=5,
        line_width=1,
        layer="below"))
    annotations_list = []
    annotations_list.append(
        dict(
            x=8000,
            y=np.median(scatter_plot_df_with_age.brewery_pure_average) - 0.1,
            xref="x",
            yref="y",
            text=f"Медианное значение: {round(np.median(scatter_plot_df_with_age.brewery_pure_average), 2)}",
            showarrow=False,
            font={
                'family':'PT Sans',
                'color':'#666666',
                'size':12
            }
        )
    )
    annotations_list.append(
        dict(
            x=np.median(scatter_plot_df_with_age.rating_count) + 180,
            y=0.8,
            xref="x",
            yref="y",
            text=f"Медианное значение: {round(np.median(scatter_plot_df_with_age.rating_count), 2)}",
            showarrow=False,
            font={
                'family':'PT Sans',
                'color':'#666666',
                'size':12
            },
            textangle=-90
        )
    )
    bucket = []
    for beer_count in scatter_plot_df_with_age.beer_count:
        if beer_count < 10:
            bucket.append(7)
        elif 10 <= beer_count <= 30:
            bucket.append(9)
        elif 31 <= beer_count <= 50:
            bucket.append(11)
        else:
            bucket.append(13)
    scatter_plot_df_with_age['bucket_beer_count'] = bucket
    bucket_age = []
    for age in scatter_plot_df_with_age.age_on_service:
        if age < 4:
            bucket_age.append(0)
        elif 4 <= age <= 6:
            bucket_age.append(1)
        elif 6 < age < 8:
            bucket_age.append(2)
        else:
            bucket_age.append(3)
    scatter_plot_df_with_age['bucket_age'] = bucket_age
    scatter_plot_df_0 = scatter_plot_df_with_age[(scatter_plot_df_with_age.bucket_age == 0)]
    scatter_plot_df_1 = scatter_plot_df_with_age[scatter_plot_df_with_age.bucket_age == 1]
    scatter_plot_df_2 = scatter_plot_df_with_age[scatter_plot_df_with_age.bucket_age == 2]
    scatter_plot_df_3 = scatter_plot_df_with_age[scatter_plot_df_with_age.bucket_age == 3]
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=scatter_plot_df_0.rating_count,
        y=scatter_plot_df_0.brewery_pure_average,
        name='< 4',
        mode='markers',
        opacity=0.85,
        text=scatter_plot_df_0.name_count,
        marker_color='rgb(114, 183, 178)',
        marker_size=scatter_plot_df_0.bucket_beer_count,
        textfont={"family":"PT Sans",
                  "color":"black"
                 }
    ))

    fig.add_trace(go.Scatter(
        x=scatter_plot_df_1.rating_count,
        y=scatter_plot_df_1.brewery_pure_average,
        name='4 – 6',
        mode='markers',
        opacity=0.85,
        marker_color='rgb(76, 120, 168)',
        text=scatter_plot_df_1.name_count,
        marker_size=scatter_plot_df_1.bucket_beer_count,
        textfont={"family":"PT Sans",
                  "color":"black"
                 }
    ))

    fig.add_trace(go.Scatter(
        x=scatter_plot_df_2.rating_count,
        y=scatter_plot_df_2.brewery_pure_average,
        name='6 – 8',
        mode='markers',
        opacity=0.85,
        marker_color='rgb(245, 133, 23)',
        text=scatter_plot_df_2.name_count,
        marker_size=scatter_plot_df_2.bucket_beer_count,
        textfont={"family":"PT Sans",
                  "color":"black"
                 }
    ))

    fig.add_trace(go.Scatter(
        x=scatter_plot_df_3.rating_count,
        y=scatter_plot_df_3.brewery_pure_average,
        name='8+',
        mode='markers',
        opacity=0.85,
        marker_color='rgb(228, 87, 86)',
        text=scatter_plot_df_3.name_count,
        marker_size=scatter_plot_df_3.bucket_beer_count,
        textfont={"family":"PT Sans",
                  "color":"black"
                 }
    ))

    fig.update_layout(
        title=f"Отношение количества отзывов к средней оценке пивоварни<br>за последние {n_days} дней, топ-{top_n} пивоварен",
        font={
                'family':'PT Sans',
                'color':'black',
                'size':14
            },
        plot_bgcolor='rgba(0,0,0,0)',
        yaxis_title="Средняя оценка",
        xaxis_title="Количество отзывов",
        legend_title_text='Возраст пивоварни<br>на Untappd, лет:',
        height=750,
        shapes=dict_list,
        annotations=annotations_list
    )
    return fig