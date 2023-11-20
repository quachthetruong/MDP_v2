import sys
from datetime import datetime

from pandas import DataFrame
import json

from pendulum import now

from .storage_base import StorageBase
from common.utils import *
from common import config
from kafka import KafkaProducer
import logging
from common.utils import get_env_var

logger = logging.getLogger('kafka')
logging.addHandler(logging.StreamHandler(sys.stdout))
logging.setLevel(logging.DEBUG)

class KafkaStorage(StorageBase):
    def __init__(self, stream_config):

        if "same_table_name" in stream_config and stream_config["same_table_name"]:
            self.table_name = stream_config["signal_name"]
        else:
            self.table_name = generated_identified_name(
                stream_config["signal_name"],
                stream_config["timestep"],
                stream_config["version"],
            )
        self.name_fields = [item[0] for item in stream_config["stream_fields"]]
        if config.SYSTEM_TIMESTAMP_COL not in self.name_fields:
            self.name_fields.append(config.SYSTEM_TIMESTAMP_COL)
        if config.SYSTEM_SYMBOL_COL not in self.name_fields:
            self.name_fields.append(config.SYSTEM_SYMBOL_COL)

        try:
            kafka_broker_url = get_env_var("KAFKA_DSAI")
        except:
            kafka_broker_url = "dsai-kafka-kafka-bootstrap:9092"
        logging.info(f'Kafka DSAI: {kafka_broker_url}')
        kafka_server = (
            kafka_broker_url
        )
        self.backend = KafkaProducer(
            bootstrap_servers=[kafka_server],
            value_serializer=stream_config["value_serializer"]
        )
        logging.info("Ready to produce messages to topic: %s", self.table_name)

        super().__init__()

    def on_send_success(self, record_metadata):
        logging.info(
            "Successfully sent record to topic %s partition [%d] at offset %d.",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    def on_send_error(self, excp):
        logging.error("I am an errback", exc_info=excp)

    def append(self, df_record: DataFrame, commit_every=1000):
        for _, row in df_record.iterrows():
            print(row)
            #row_ = row.drop(labels=[config.SYSTEM_TIMESTAMP_COL, config.SYSTEM_SYMBOL_COL])
            print("ROWW: ", row.to_dict())
            if row.isnull().values.any():
                logging.warning(
                    "There's a null value in the row. The row will be skipped: %s",
                    row.to_dict(),
                )
                continue

            # convert to raw new template
            event = row.to_dict()
            out_dict = {}
            out_dict[config.SYSTEM_SYMBOL_COL] = event[config.SYSTEM_SYMBOL_COL]
            out_dict[config.SYSTEM_TIMESTAMP_COL] = str(event[config.SYSTEM_TIMESTAMP_COL])
            out_dict['publish_time'] = datetime.strftime(now(), "%Y-%m-%d %H:%M:%S")
            out_dict["title"] = event['title']
            out_dict["content"] = event['content']
            out_dict["summary"] = event['summary']
            out_dict["new_type"] = 'bctc'
            out_dict["domain"] = 'dnse'
            out_dict["is_crawled"] = False

            out_dict["url"] = None
            out_dict["word_count"] =  len(event['summary'].split())
            out_dict["symbols"] =",".join(event['symbols'])
            out_dict["language_id"] = 1
            out_dict["list_symbols"] = event['symbols']
            out_dict["source"] = None
            out_dict["head"] = event['summary']
            out_dict["head_image_url"] = None
            out_dict["thumb_image_url"] = None
            out_dict["tag"] = None
            out_dict["author"] = 'DNSE'
            self.backend.send(self.table_name, json.dumps(out_dict)).add_callback(
                self.on_send_success
            ).add_errback(self.on_send_error)
        self.backend.flush()
        return
