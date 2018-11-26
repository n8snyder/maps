from invoke import exceptions, run, task
from models import City, AirQuality, db
import os
import time
import arrow
import requests
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
