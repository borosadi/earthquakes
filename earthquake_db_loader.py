import pytz
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root:password@localhost:3306/earthquake")

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Earthquakes():

    def __init__(self, engine) -> None:

        self.db_engine = engine

    def load_df_to_db(self):

        for month in range(1,13):
            prop = self.create_properties_df(events=self.get_monthly_events(month=month))
            prop.to_sql(name='Properties', con=self.db_engine, if_exists='append', index=False)
            logger.info(f'{month}. month loaded to Properties table')

            geo = self.create_geometry_df(events=self.get_monthly_events(month=month))
            geo.to_sql(name='Geometry', con=self.db_engine, if_exists='append', index=False)
            logger.info(f'{month}. month loaded to Geometry table')

    @staticmethod
    def get_monthly_events(month: int) -> json:

        if month > 12:
            raise Exception('There are only 12 months')
        elif month == 12:
            resp = requests.get(f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2017-12-01&endtime=2018-01-01")
            return resp.json()
        else:
            resp = requests.get(f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2017-{month}-01&endtime=2017-{month+1}-01")
            return resp.json()

    def get_ids(self, events) -> list:

        return [id.get('id') for id in events['features']]

    def create_geometry_df(self, events: json) -> pd.DataFrame:

        coordinates = [coordinates.get('geometry').get('coordinates') for coordinates in events['features']]
        longitude = [lo[0]for lo in coordinates]
        latitude = [la[1] for la in coordinates]
        depth = [de[2] for de in coordinates]
        geo = pd.DataFrame(data=list(zip(self.get_ids(events=events), longitude, latitude, depth)),index=None,\
            columns=['id', 'longitude', 'latitude', 'depth'])
        return geo

    def create_properties_df(self, events: json) -> pd.DataFrame:

        properties = [properties.get('properties') for properties in events['features']]
        prop = pd.DataFrame(data=properties, index=None)
        prop.insert(loc=0, column='id', value=self.get_ids(events=events))
        # time conversion
        prop.loc[:, 'time'] = prop['time'].apply(lambda x: datetime.fromtimestamp(x/1e3))
        prop.loc[:, 'updated'] = prop['updated'].apply(lambda x: datetime.fromtimestamp(x/1e3))
        # timezone offset
        prop.loc[prop['tz'].notna(), 'tz'] = prop.loc[prop['tz'].notna(), 'tz']\
            .apply(lambda x: timedelta(hours=int(x / 100), minutes=x - (int(x /100) * 100)))
        prop.loc[:, 'tz'].fillna(timedelta(0), inplace=True)  
        prop.loc[prop['tz'].notna(), 'time'] = prop['time'] - prop['tz']
        prop.drop(columns=['tz'], inplace=True)
        return prop


if __name__ == '__main__':
    eq = Earthquakes(engine=engine)
    eq.load_df_to_db()
