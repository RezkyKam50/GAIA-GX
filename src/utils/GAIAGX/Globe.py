from dash import (
    Dash, 
    html, 
    dcc, 
    Input, 
    Output, 
    State, 
    callback, 
    MATCH, 
    ALL,
    clientside_callback,
    ctx
)
import plotly.graph_objects as go, pandas, cudf
from events.weather import get_major_cities_weather
from events.population import fetch_live_population_data, fetch_rss_feeds
from events.seismic import recent_events
from events.tide import fetch_tide_stations


@callback(
    Output('earth-globe', 'figure'),
    Input('news-data-store', 'data'),  # Trigger on initial data load
    prevent_initial_call=False
)
def update_base_globe(news_data):
    population_data = fetch_live_population_data()
    significant_countries = {k: v for k, v in population_data.items() if v > 5}
    countries = list(significant_countries.keys())
    populations = list(significant_countries.values())
    
    # Use cuDF instead of pandas for GPU acceleration
    df_globe = cudf.DataFrame({
        'country': list(significant_countries.keys()),
        'population': list(significant_countries.values())
    })
    
    df_globe['hover_text'] = df_globe['country'] + ': ' + df_globe['population'].astype('str') + ' million people'
    
    # Convert back to pandas for Plotly compatibility if needed
    df_globe = df_globe.to_pandas()

    fig = go.Figure(data=go.Choropleth(
        locations=df_globe['country'],
        z=df_globe['population'],
        text=df_globe['hover_text'],
        colorscale='twilight',
        autocolorscale=False,
        reversescale=False,
        marker_line_width=0,
        colorbar_title="Population<br>(Millions)",
        colorbar=dict(
            bgcolor='rgba(26, 31, 36, 0.8)',
            tickfont=dict(color='#00ffaf')
        ),
        hoverinfo='text'
    ))

    fig.update_geos(
        projection_type='natural earth',
        showland=True,
        landcolor='#2A3238',
        oceancolor='#1a1f24',
        showocean=True,
        showcountries=False,
        showcoastlines=True,
        coastlinecolor='#00ffaf',
        coastlinewidth=1,
        showframe=False,
        projection_rotation=dict(lon=0, lat=0),  # Default center
        bgcolor='rgba(0,0,0,0)',
        resolution=50
    )
    
    fig.update_layout(
        height=1200,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#00ffaf', size=12),
        margin=dict(l=0, r=0, t=50, b=0),
        legend=dict(
            x=0,
            y=1,
            xanchor='left',
            yanchor='top',
            bgcolor='rgba(26, 31, 36, 0.8)',
            font=dict(color='#00ffaf')
        ),
        uirevision='constant',
        hovermode='closest',
        # Configure drag interactions - moved from update_geos
        dragmode=False,  # Disable default drag behavior
        # For geo plots, we need to use specific geo configuration
        geo=dict(
            # Lock the view to prevent vertical movement
            center=dict(lat=0, lon=0),
            projection_rotation=dict(lon=0, lat=0, roll=0),
            # Disable user interaction that would allow vertical movement
            # This is achieved by not enabling any drag modes that allow tilt
        )
    )
    
    # Add custom configuration to restrict movement
    fig.update_layout(
        # Disable the ability to tilt/rotate vertically
        scene=dict(
            camera=dict(
                up=dict(x=0, y=0, z=1),
                eye=dict(x=0, y=0, z=1.5),
                projection=dict(type="orthographic")
            )
        ) if 'scene' in fig.to_dict() else {}
    )
    
    return fig

@callback(
    Output('news-data-store', 'data'),
    Input('earth-globe', 'id'),  # Trigger on initial load
    prevent_initial_call=False
)
def fetch_news_data(globe_id):
    try:
        rss_data = fetch_rss_feeds()
        if rss_data:
            return {
                'lats': [item['lat'] for item in rss_data],
                'lons': [item['lon'] for item in rss_data],
                'texts': [
                    f"<b>{item['title']}</b><br>{item['published']}<br>{item['summary']}" +
                    (f"<br><br>🌡️ <b>Current Weather:</b><br>"
                     f"{item['weather_icon']} {item['weather_description']}<br>"
                     f"Temperature: {item['temperature']}<br>"
                     f"Humidity: {item['humidity']}<br>"
                     f"Precipitation: {item['precipitation']}" if 'temperature' in item else "") +
                    f"<br><a href='{item['link']}' target='_blank'>Read more</a>"
                    for item in rss_data
                ]
            }
    except:
        pass
    return None

@callback(
    Output('weather-data-store', 'data'),
    Input('earth-globe', 'id'),  # Trigger on initial load
    prevent_initial_call=False
)
def fetch_weather_data(globe_id):
    try:
        cities_weather = get_major_cities_weather()
        if cities_weather:
            return {
                'lats': [city['lat'] for city in cities_weather],
                'lons': [city['lon'] for city in cities_weather],
                'texts': [
                    f"<b>{city['name']}</b><br>"
                    f"{city['weather_icon']} {city['weather_description']}<br>"
                    f"🌡️ Temperature: {city['temperature']:.1f}°C<br>"
                    f"💧 Humidity: {city['humidity']:.0f}%<br>"
                    f"🌧️ Precipitation: {city['precipitation']:.1f}mm"
                    for city in cities_weather
                ]
            }
    except:
        pass
    return None

@callback(
    Output('earthquake-data-store', 'data'),
    Input('earth-globe', 'id'),  # Trigger on initial load
    prevent_initial_call=False
)
def fetch_earthquake_data(globe_id):
    try:
        if recent_events:
            return {
                'lats': [e['lat'] for e in recent_events],
                'lons': [e['lon'] for e in recent_events],
                'mags': [e['mag'] for e in recent_events],
                'texts': [
                    f"🌎 <b>{e['region']}</b><br>"
                    f"M{e['mag']:.1f} at depth {e['depth']:.1f} km<br>"
                    f"{e['time']}"
                    for e in recent_events
                ]
            }
    except:
        pass
    return None

@callback(
    Output('tide-data-store', 'data'),
    Input('earth-globe', 'id'),  # Trigger on initial load
    prevent_initial_call=False
)
def fetch_tide_data(globe_id):
    try:
        tide_stations = fetch_tide_stations(max_stations=50)
        if tide_stations:
            return {
                'lats': [t['lat'] for t in tide_stations],
                'lons': [t['lon'] for t in tide_stations],
                'texts': [
                    f"<b>🌊 {t['name']}</b><br>{t['predictions']}<br>"
                    f"<a href='https://surftruths.com/api/tide/stations/{t['id']}.json' target='_blank'>View Station</a>"
                    for t in tide_stations
                ],
                'ids': [t['id'] for t in tide_stations]
            }
    except:
        pass
    return None

@callback(
    Output('earth-globe', 'figure', allow_duplicate=True),
    [Input('news-data-store', 'data'),
     Input('weather-data-store', 'data'),
     Input('earthquake-data-store', 'data'),
     Input('tide-data-store', 'data')],
    State('earth-globe', 'figure'),
    prevent_initial_call=True
)
def add_data_layers(news_data, weather_data, earthquake_data, tide_data, current_fig):
    if current_fig is None:
        return current_fig
    
    fig = go.Figure(current_fig)
    
    fig.data = [trace for trace in fig.data if trace.type == 'choropleth']
    
    # Add news feed layer
    if news_data:
        fig.add_trace(go.Scattergeo(
            lon=news_data['lons'],
            lat=news_data['lats'],
            text=news_data['texts'],
            mode='markers',
            hoverinfo='text',
            name='News Feed',
            showlegend=True,
            marker=dict(
                size=8,
                color='#00ffaf',
                symbol='circle'
            )
        ))
    
    # Add weather layer
    if weather_data:
        fig.add_trace(go.Scattergeo(
            lon=weather_data['lons'],
            lat=weather_data['lats'],
            text=weather_data['texts'],
            mode='markers',
            hoverinfo='text',
            name='Weather Stations',
            showlegend=True,
            marker=dict(
                size=15,
                color='#ff6b6b',
                symbol='arrow',
                line=dict(width=1, color='white')
            )
        ))
    
    # Add earthquake layer
    if earthquake_data:
        fig.add_trace(go.Scattergeo(
            lon=earthquake_data['lons'],
            lat=earthquake_data['lats'],
            text=earthquake_data['texts'],
            mode='markers',
            hoverinfo='text',
            name='Earthquakes',
            marker=dict(
                size=[max(4, m * 2) for m in earthquake_data['mags']],
                color='red',
                opacity=0.7,
                line=dict(width=1, color='white'),
                symbol='circle'
            )
        ))
    
    # Add tide layer
    if tide_data:
        fig.add_trace(go.Scattergeo(
            lon=tide_data['lons'],
            lat=tide_data['lats'],
            text=tide_data['texts'],
            mode='markers',
            hoverinfo='text',
            name='Tide Stations',
            marker=dict(
                size=12,
                color='blue',
                symbol='triangle-up',
                line=dict(width=1, color='white')
            )
        ))
    
    return fig
 