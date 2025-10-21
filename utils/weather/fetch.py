import openmeteo_requests
import requests_cache
from retry_requests import retry
import cudf, cupy as cp # gpu accel
from datetime import datetime

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
    cities = [
        # North America
        {"name": "New York", "lat": 40.71, "lon": -74.01},
        {"name": "Los Angeles", "lat": 34.05, "lon": -118.25},
        {"name": "Chicago", "lat": 41.88, "lon": -87.63},
        {"name": "Toronto", "lat": 43.65, "lon": -79.38},
        {"name": "Mexico City", "lat": 19.43, "lon": -99.13},
        {"name": "Vancouver", "lat": 49.28, "lon": -123.12},
        {"name": "Miami", "lat": 25.76, "lon": -80.19},
        {"name": "Montreal", "lat": 45.50, "lon": -73.57},
        {"name": "San Francisco", "lat": 37.77, "lon": -122.42},
        {"name": "Denver", "lat": 39.74, "lon": -104.99},
        {"name": "Boston", "lat": 42.36, "lon": -71.06},
        {"name": "Washington D.C.", "lat": 38.91, "lon": -77.04},
        {"name": "Seattle", "lat": 47.61, "lon": -122.33},
        {"name": "Atlanta", "lat": 33.75, "lon": -84.39},
        {"name": "Dallas", "lat": 32.78, "lon": -96.80},
        {"name": "Houston", "lat": 29.76, "lon": -95.37},
        {"name": "Calgary", "lat": 51.05, "lon": -114.07},
        {"name": "Ottawa", "lat": 45.42, "lon": -75.70},
        {"name": "Guadalajara", "lat": 20.66, "lon": -103.35},
        {"name": "Havana", "lat": 23.11, "lon": -82.37},

        # South America
        {"name": "São Paulo", "lat": -23.55, "lon": -46.63},
        {"name": "Buenos Aires", "lat": -34.60, "lon": -58.38},
        {"name": "Rio de Janeiro", "lat": -22.91, "lon": -43.17},
        {"name": "Lima", "lat": -12.04, "lon": -77.03},
        {"name": "Bogotá", "lat": 4.71, "lon": -74.07},
        {"name": "Santiago", "lat": -33.45, "lon": -70.65},
        {"name": "Montevideo", "lat": -34.90, "lon": -56.19},
        {"name": "Quito", "lat": -0.18, "lon": -78.47},
        {"name": "Caracas", "lat": 10.49, "lon": -66.88},
        {"name": "La Paz", "lat": -16.50, "lon": -68.12},
        {"name": "Brasília", "lat": -15.79, "lon": -47.88},
        {"name": "Salvador", "lat": -12.97, "lon": -38.51},
        {"name": "Fortaleza", "lat": -3.73, "lon": -38.52},
        {"name": "Medellín", "lat": 6.24, "lon": -75.57},
        {"name": "Guayaquil", "lat": -2.19, "lon": -79.89},
        {"name": "Valparaíso", "lat": -33.05, "lon": -71.62},

        # Europe
        {"name": "London", "lat": 51.51, "lon": -0.13},
        {"name": "Paris", "lat": 48.85, "lon": 2.35},
        {"name": "Berlin", "lat": 52.52, "lon": 13.41},
        {"name": "Rome", "lat": 41.90, "lon": 12.50},
        {"name": "Madrid", "lat": 40.42, "lon": -3.70},
        {"name": "Moscow", "lat": 55.76, "lon": 37.62},
        {"name": "Amsterdam", "lat": 52.37, "lon": 4.90},
        {"name": "Prague", "lat": 50.08, "lon": 14.43},
        {"name": "Vienna", "lat": 48.21, "lon": 16.37},
        {"name": "Dublin", "lat": 53.35, "lon": -6.26},
        {"name": "Lisbon", "lat": 38.72, "lon": -9.14},
        {"name": "Oslo", "lat": 59.91, "lon": 10.75},
        {"name": "Budapest", "lat": 47.50, "lon": 19.04},
        {"name": "Istanbul", "lat": 41.01, "lon": 28.98},
        {"name": "Athens", "lat": 37.98, "lon": 23.73},
        {"name": "Warsaw", "lat": 52.23, "lon": 21.01},
        {"name": "Brussels", "lat": 50.85, "lon": 4.35},
        {"name": "Stockholm", "lat": 59.33, "lon": 18.07},
        {"name": "Copenhagen", "lat": 55.68, "lon": 12.57},
        {"name": "Helsinki", "lat": 60.17, "lon": 24.94},
        {"name": "Munich", "lat": 48.14, "lon": 11.58},
        {"name": "Milan", "lat": 45.46, "lon": 9.19},
        {"name": "Barcelona", "lat": 41.39, "lon": 2.16},
        {"name": "Edinburgh", "lat": 55.95, "lon": -3.19},
        {"name": "Zurich", "lat": 47.38, "lon": 8.54},

        # Africa
        {"name": "Cairo", "lat": 30.04, "lon": 31.24},
        {"name": "Lagos", "lat": 6.46, "lon": 3.39},
        {"name": "Nairobi", "lat": -1.29, "lon": 36.82},
        {"name": "Johannesburg", "lat": -26.20, "lon": 28.04},
        {"name": "Accra", "lat": 5.56, "lon": -0.20},
        {"name": "Addis Ababa", "lat": 9.03, "lon": 38.74},
        {"name": "Casablanca", "lat": 33.57, "lon": -7.59},
        {"name": "Cape Town", "lat": -33.92, "lon": 18.42},
        {"name": "Kinshasa", "lat": -4.44, "lon": 15.27},
        {"name": "Luanda", "lat": -8.84, "lon": 13.23},
        {"name": "Khartoum", "lat": 15.50, "lon": 32.56},
        {"name": "Dar es Salaam", "lat": -6.82, "lon": 39.27},
        {"name": "Abidjan", "lat": 5.32, "lon": -4.02},
        {"name": "Alexandria", "lat": 31.20, "lon": 29.92},
        {"name": "Marrakesh", "lat": 31.63, "lon": -8.01},
        {"name": "Dakar", "lat": 14.69, "lon": -17.45},
        {"name": "Kampala", "lat": 0.35, "lon": 32.58},

        # Asia
        {"name": "Tokyo", "lat": 35.69, "lon": 139.69},
        {"name": "Beijing", "lat": 39.90, "lon": 116.41},
        {"name": "Seoul", "lat": 37.57, "lon": 126.98},
        {"name": "Bangkok", "lat": 13.75, "lon": 100.50},
        {"name": "Mumbai", "lat": 19.08, "lon": 72.88},
        {"name": "Delhi", "lat": 28.61, "lon": 77.21},
        {"name": "Singapore", "lat": 1.35, "lon": 103.82},
        {"name": "Jakarta", "lat": -6.21, "lon": 106.85},
        {"name": "Kuala Lumpur", "lat": 3.14, "lon": 101.69},
        {"name": "Manila", "lat": 14.60, "lon": 120.98},
        {"name": "Shanghai", "lat": 31.23, "lon": 121.47},
        {"name": "Hong Kong", "lat": 22.32, "lon": 114.17},
        {"name": "Taipei", "lat": 25.03, "lon": 121.57},
        {"name": "Osaka", "lat": 34.69, "lon": 135.50},
        {"name": "Kyoto", "lat": 35.01, "lon": 135.77},
        {"name": "Dubai", "lat": 25.20, "lon": 55.27},
        {"name": "Riyadh", "lat": 24.71, "lon": 46.68},
        {"name": "Tel Aviv", "lat": 32.09, "lon": 34.78},
        {"name": "Ho Chi Minh City", "lat": 10.82, "lon": 106.63},
        {"name": "Hanoi", "lat": 21.03, "lon": 105.85},
        {"name": "Dhaka", "lat": 23.71, "lon": 90.41},
        {"name": "Karachi", "lat": 24.86, "lon": 67.01},
        {"name": "Colombo", "lat": 6.93, "lon": 79.85},
        {"name": "Kathmandu", "lat": 27.72, "lon": 85.32},


        # Oceania
        {"name": "Sydney", "lat": -33.87, "lon": 151.21},
        {"name": "Melbourne", "lat": -37.81, "lon": 144.96},
        {"name": "Auckland", "lat": -36.85, "lon": 174.76},
        {"name": "Wellington", "lat": -41.29, "lon": 174.78},
        {"name": "Brisbane", "lat": -27.47, "lon": 153.03},
        {"name": "Perth", "lat": -31.95, "lon": 115.86},
        {"name": "Adelaide", "lat": -34.93, "lon": 138.60},
        {"name": "Gold Coast", "lat": -28.02, "lon": 153.40},
        {"name": "Christchurch", "lat": -43.53, "lon": 172.62},
        {"name": "Suva", "lat": -18.14, "lon": 178.44},
        {"name": "Port Moresby", "lat": -9.44, "lon": 147.18},
    ]
    
    days_history = 93  # Max free tier allows
    
    print(f" Collecting {days_history} days of hourly weather data...\n")
    
    all_data = []
    for city in cities:
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
        
        filename = f"./utils/weather/weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        final_df.to_csv(filename, index=False)
        
        print(f"\n Saved {len(final_df)} records to '{filename}'")
        print(f"   Cities: {len(cities)}")
        print(f"   Time range: {days_history} days + 7 day forecast")
        print(f"   Weather features: 6 | Temporal features: 16")
        print(f"\n Sample:")
        print(final_df[['timestamp', 'city', 'temperature', 'hour', 'hour_sin', 'hour_cos']].head(3).to_pandas())
    else:
        print("No data collected...")

if __name__ == "__main__":
    main()