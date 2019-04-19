from invoke import exceptions, run, task
from models import City, AirQuality, db
import os
import time
import arrow
import requests
import folium
from peewee import DoesNotExist

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


@task
def init_db(ctx):
    db.connect()
    db.drop_tables([City, AirQuality])
    db.create_tables([City, AirQuality])


CITY_NAMES = ['Alameda', 'Berkeley', 'Concord', 'Gilroy', 'Livermore', 'Sacramento',
              'Oakland', 'Napa', 'Santa Cruz', 'San Francisco', 'Richmond', 'San Jose',
              'Redwood City', 'Vallejo', 'Stockton']
KEY = os.environ['AIR_KEY']


def fetch_response(url, params):
    while True:
        response = requests.get(url, params=params)
        try:
            message = response.json()['data']['message']
        except KeyError:
            logger.info(response)
            return response
        except ValueError:
            logger.error('ValueError error')
            logger.error(response)
            logger.error(response.content)
            pass
        else:
            if message == 'call_per_minute_limit_reached':
                logger.info('calls per minute limit reached')
                logger.info('waiting 10 seconds then trying again')
                time.sleep(10)
                continue
            else:
                logger.info(message)


@task
def download_city_data(ctx):
    base_params = {'key': KEY}
    url = "http://api.airvisual.com/v2/city"
    for city_name in CITY_NAMES:
        response = fetch_response(url, params={**base_params, 'country': 'USA',
                                               'state': 'California', 'city': city_name})
        city = City.create(
            name=city_name,
            lat=response.json()['data']['location']['coordinates'][1],
            lon=response.json()['data']['location']['coordinates'][0],
        )
        logger.info(f'Saved {city}')


@task
def download_air_data(ctx):
    base_params = {'key': KEY}
    url = "http://api.airvisual.com/v2/city"
    for city_name in CITY_NAMES:
        response = fetch_response(url, params={**base_params, 'country': 'USA',
                                               'state': 'California', 'city': city_name})
        city = City.select().where(City.name == city_name).get()
        aqi = response.json()['data']['current']['pollution']['aqius']
        timestamp = response.json()['data']['current']['pollution']['ts']
        timestamp = arrow.get(timestamp).datetime
        try:
            air_data = AirQuality.select().where(AirQuality.city == city,
                                                 AirQuality.timestamp == timestamp).get()
        except DoesNotExist:
            air_quality = AirQuality.create(
                city=city, aqi=aqi, timestamp=timestamp)
            logger.info(f'Saved {air_quality}')
        else:
            logger.info(f'Data already exists: {air_data}')

def get_color(aqi):
    if aqi < 51:
        color = "#00d200"
    elif aqi < 101:
        color = "#feff00"
    elif aqi < 151:
        color = "#fe7100"
    elif aqi < 201:
        color = "#fe0400"
    elif aqi < 301:
        color = "#a30055"
    elif aqi < 501:
        color = "#890027"
    else:
        color = "#000000"
    return color

@task
def create_map(ctx):
    city_data = City.select()
    air_data = []
    for city in city_data:
        air_quality = AirQuality.select(AirQuality, City).join(City).where(AirQuality.city==city).order_by(-AirQuality.timestamp).first()
        air_data.append(air_quality)
    m = folium.Map(location=[37.864767,-122.302741], zoom_start=8)
    for air in air_data:
        radius = 5000
        color = get_color(air.aqi)
        popup_text = (f'{air.city.name}<br>'
                    f'AQI: {air.aqi}<br>'
                    f"Last updated {arrow.get(air.timestamp).humanize()}")
        folium.Circle(
            location=[air.city.lat, air.city.lon],
            radius=radius,
            color=color,
            fill=True,
            popup=popup_text
        ).add_to(m)
    m.save(outfile="map.html")
    logger.info("Finished creating map")
    # m.save(outfile=f"{slugify(str(datetime.now()))}.html")