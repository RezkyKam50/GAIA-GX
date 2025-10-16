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
from utils.config.config import (
    PLOT_HEIGHT, 
    NEAREST_NEIGHBORS, 
    MAX_SUGGESTIONS
)
import dash_bootstrap_components as dbc

from datetime import datetime, timedelta

app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], assets_folder='assets')

app.title = "GAIA-GX"
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Img(
                    src="/assets/Logo.png",
                    style={
                        "height": "100px",
                        "marginRight": "15px"
                    }
                ),
                html.H1(
                "The Nutritionist Analytics Dashboard.",
                className="company-text"
                )
            ], className="app-title d-flex align-items-center justify-content-center my-4")
        ])
    ]),
    
    # Globe Section 
    dbc.Row([
        dbc.Col([
            html.H2("GAIA-GX", style={'color': '#00ffaf', 'marginBottom': '20px'})
        ])
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
 
                    html.Div(className="cosmic-dust"),
                    html.Div(className="galaxy-bg"),
                    
                    # Hidden data stores for progressive loading
                    dcc.Store(id='news-data-store'),
                    dcc.Store(id='weather-data-store'),
                    dcc.Store(id='earthquake-data-store'),
                    dcc.Store(id='tide-data-store'),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Select Region:", style={'color': 'white'}),
                            dcc.Dropdown(
                                id='region-selector',
                                options=[
                                    {'label': 'North America', 'value': 'NA'},
                                    {'label': 'South America', 'value': 'SA'},
                                    {'label': 'Europe', 'value': 'EU'},
                                    {'label': 'Africa', 'value': 'AF'},
                                    {'label': 'Asia', 'value': 'AS'},
                                    {'label': 'Oceania', 'value': 'OC'}
                                ],
                                value='NA',
                                style={
                                    'backgroundColor': '#3A4249',
                                    'color': '#00ffaf',
                                    'borderColor': '#00ffaf'
                                },
                                className="mb-3"
                            )
                        ], width=6),
                        dbc.Col([
                            dbc.Label("Visualization Mode:", style={'color': 'white'}),
                            dcc.Dropdown(
                                id='globe-mode',
                                options=[
                                    {'label': 'Geographic', 'value': 'geo'},
                                    {'label': 'Orthographic', 'value': 'ortho'},
                                    {'label': 'Natural Earth', 'value': 'natural'}
                                ],
                                value='ortho',
                                style={
                                    'backgroundColor': '#3A4249',
                                    'color': '#00ffaf',
                                    'borderColor': '#00ffaf'
                                },
                                className="mb-3"
                            )
                        ], width=6)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dcc.Graph(
                                id='earth-globe',
                                style={'height': '1200px'},
                                config={'displayModeBar': True, 'scrollZoom': True}
                            )
                        ])
                    ])
                ], className="globe-container")
            ], className='globe-card', style={'backgroundColor': '#2A3238', 'border': '2px solid #00ffaf', 'position': 'relative'})
        ])
    ], className="mb-4"),

    html.Hr(style={'borderColor': '#00ffaf', 'marginTop': '40px', 'marginBottom': '20px'}),
], fluid=True)

import utils.GAIAGX.Globe 

if __name__ == '__main__':
    app.run(debug=True, port=8080)