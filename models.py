from peewee import *

db = SqliteDatabase('air.db')


class City(Model):
    name = CharField()
    lon = DecimalField()
    lat = DecimalField()

    class Meta:
        database = db

    def __repr__(self):
        return f'<City: {self.name}>'

    def __str__(self):
        return self.__repr__()


class AirQuality(Model):
    city = ForeignKeyField(City, backref='air_qualities')
    aqi = IntegerField()
    timestamp = DateTimeField()

    class Meta:
        database = db

    def __repr__(self):
        return f'<AirQuality: {self.city}, {self.aqi}>'

    def __str__(self):
        return self.__repr__()
