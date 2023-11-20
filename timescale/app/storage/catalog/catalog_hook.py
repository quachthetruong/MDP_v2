import logging
import config
from pandas import DataFrame
import requests


class CatalogHook:
    def __init__(self):
        self.catalog_url = config.CATALOG_SERVICE_URL

    def get(self, endpoint):
        try:
            res = requests.get(self.catalog_url+endpoint)
            # logging.info(f"LOG response : {res.text}")
            res.raise_for_status()
        except Exception as err:
            logging.error(f"LOG error: {str(err)}")
            raise
        return res.json()

    def get_all(self, endpoint):
        result = []
        page_number = 1
        try:
            while True:
                res = requests.get(self.catalog_url+endpoint +
                                   f"?page_number={page_number}")
                # logging.info(f"LOG response : {res.text}")
                res.raise_for_status()
                res = res.json()
                if len(res) == 0:
                    break
                page_number += 1
                result.extend(res)
                # logging.info(f"LOG result : {len(result)}")
        except Exception as err:
            logging.error(f"LOG error: {str(err)}")
            raise
        return result

    def post(self, endpoint, data):
        try:
            res = requests.post(self.catalog_url+endpoint, json=data)
            logging.info(f"LOG response : {res.text}")
            res.raise_for_status()
        except Exception as err:
            logging.error(f"LOG error: {str(err)}")
            raise
        return res.json()
