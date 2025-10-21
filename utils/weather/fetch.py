import openmeteo_requests
import requests_cache
from retry_requests import retry
import cudf, cupy as cp # gpu accel
from datetime import datetime

import cities

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def fetch_historical_weather(latitude, longitude, days=90):
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", 
                      "wind_speed_10m", "pressure_msl", "cloud_cover"],
            "past_days": days,
            "forecast_days": 7,
            "timezone": "auto"
        }
        
        response = openmeteo.weather_api(url, params=params)[0]
        hourly = response.Hourly()

        data = {
            "timestamp": cudf.to_datetime(
                [datetime.fromtimestamp(t) for t in range(
                    hourly.Time(), hourly.TimeEnd(), hourly.Interval()
                )]
            ),
            "temperature": hourly.Variables(0).ValuesAsNumpy(),
            "humidity": hourly.Variables(1).ValuesAsNumpy(),
            "precipitation": hourly.Variables(2).ValuesAsNumpy(),
            "wind_speed": hourly.Variables(3).ValuesAsNumpy(),
            "pressure": hourly.Variables(4).ValuesAsNumpy(),
            "cloud_cover": hourly.Variables(5).ValuesAsNumpy()
        }
        
        return cudf.DataFrame(data)
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def add_cyclical_features(df):

    df['year'] = df['timestamp'].dt.year

    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day

    df['hour'] = df['timestamp'].dt.hour
    df['minute'] = df['timestamp'].dt.minute
    df['second'] = df['timestamp'].dt.second

    df['month_sin'] = cp.sin(2 * cp.pi * df['month'] / 12)
    df['month_cos'] = cp.cos(2 * cp.pi * df['month'] / 12)
    
    df['day_sin'] = cp.sin(2 * cp.pi * df['day'] / 31)
    df['day_cos'] = cp.cos(2 * cp.pi * df['day'] / 31)
    
    df['hour_sin'] = cp.sin(2 * cp.pi * df['hour'] / 24)
    df['hour_cos'] = cp.cos(2 * cp.pi * df['hour'] / 24)
    
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['day_of_week_sin'] = cp.sin(2 * cp.pi * df['day_of_week'] / 7)
    df['day_of_week_cos'] = cp.cos(2 * cp.pi * df['day_of_week'] / 7)
    
    df['day_of_year'] = df['timestamp'].dt.dayofyear
    df['day_of_year_sin'] = cp.sin(2 * cp.pi * df['day_of_year'] / 365)
    df['day_of_year_cos'] = cp.cos(2 * cp.pi * df['day_of_year'] / 365)
    
    return df

def collect_city_data(city_name, lat, lon, days=90):
    print(f"Fetching {city_name}...")
    df = fetch_historical_weather(lat, lon, days)
    
    if df is not None:
        df['city'] = city_name
        df['lat'] = lat
        df['lon'] = lon
        return df
    return None

def main():    
    days_history = 93  # Max free tier allows
    
    print(f" Collecting {days_history} days of hourly weather data...\n")
    
    all_data = []
    for city in cities.cities:
        df = collect_city_data(city["name"], city["lat"], city["lon"], days_history)
        if df is not None:
            all_data.append(df)
    
    if all_data:
        final_df = cudf.concat(all_data, ignore_index=True)
        
        final_df = add_cyclical_features(final_df)
        
        cols = ['timestamp', 'city', 'lat', 'lon',
                'year', 'month', 'day', 'hour', 'minute', 'second',
                'month_sin', 'month_cos', 'day_sin', 'day_cos',
                'hour_sin', 'hour_cos', 'day_of_week', 'day_of_week_sin', 
                'day_of_week_cos', 'day_of_year', 'day_of_year_sin', 'day_of_year_cos',
                'temperature', 'humidity', 'precipitation', 'wind_speed', 
                'pressure', 'cloud_cover']
        final_df = final_df[cols]
        
        filename = f"./utils/weather/weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        final_df.to_parquet(
            filename, 
            index=False
        )

        print(f"\n Saved {len(final_df)} records to '{filename}'")
        print(f"   Cities: {len(cities)}")
        print(f"   Time range: {days_history} days + 7 day forecast")
        print(f"   Weather features: 6 | Temporal features: 16")
        print(f"\n Head Sample:")
        print(final_df.head(3).to_pandas())
        print(f"\n Tail Sample:")
        print(final_df.tail(3).to_pandas())
        print(f"\n Summary:")
        print(final_df.describe())
    else:
        print("No data collected...")

if __name__ == "__main__":
    main()