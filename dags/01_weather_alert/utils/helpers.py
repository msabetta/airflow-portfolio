import os
import requests

def fetch_weather(city: str, api_key: str = None) -> dict:
    """Fetches current weather data from OpenWeatherMap API."""
    api_key = api_key or os.environ.get('OPENWEATHER_API_KEY', 'demo')
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def check_extreme_weather(weather_data: dict) -> dict:
    """Analyzes weather data and returns alerts if conditions are extreme."""
    alerts = []
    temp = weather_data.get('main', {}).get('temp', 0)
    wind_speed = weather_data.get('wind', {}).get('speed', 0)
    weather_main = weather_data.get('weather', [{}])[0].get('main', '')
    city = weather_data.get('name', 'Unknown')
    
    if temp > 40:
        alerts.append(f"EXTREME HEAT in {city}: {temp} C")
    elif temp < -10:
        alerts.append(f"EXTREME COLD in {city}: {temp} C")
    
    if wind_speed > 20:
        alerts.append(f"HIGH WIND in {city}: {wind_speed} m/s")
    
    if weather_main in ('Thunderstorm', 'Tornado'):
        alerts.append(f"SEVERE WEATHER in {city}: {weather_main}")
        
    return {
        'city': city,
        'temperature': temp,
        'wind_speed': wind_speed,
        'condition': weather_main,
        'alerts': alerts,
        'has_alert': len(alerts) > 0
    }
