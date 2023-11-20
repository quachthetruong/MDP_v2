from common import config
import sys
from sqlalchemy import create_engine, insert, select, desc, MetaData, Table
import logging
from slugify import slugify
from airflow.models import Variable


class SensesNewsStream:
    def __init__(self, connection_string, articles_table_name, stock_news_table_name, schema, start_article_id,
                 company_type, quarter) -> None:
        self.schema = schema
        self.connection_string = connection_string
        self.articles_table_name = articles_table_name
        self.stock_news_table_name = stock_news_table_name
        self.start_article_id = start_article_id
        # create connection
        self.create_table_connection()
        # get current article id
        self.current_id = self.get_current_id()
        self.company_type = company_type
        self.sense_home_url = Variable.get("SENSE_HOME_URL")
        self.quarter = quarter

    def create_table_connection(self):
        self.psql_engine = create_engine(self.connection_string, pool_pre_ping=True)
        self.meta_data = MetaData(bind=self.psql_engine)
        MetaData.reflect(self.meta_data)
        self.article_table = self.meta_data.tables[self.articles_table_name]
        self.article_table.schema = self.schema
        self.symbol_table = self.meta_data.tables[self.stock_news_table_name]
        self.symbol_table.schema = self.schema

    def insert_sense_article_stock_new_from_dict(self, event):
        with self.psql_engine.connect() as connection:
            # start transaction
            with connection.begin() as transaction:
                try:
                    print('ARTICLE: ', event)
                    # insert into article table
                    article_insert_st = (
                        self.article_table.insert()
                        .returning(self.article_table.c.id)
                        .values(
                            article_id=event["article_id"],
                            title=event["title"],
                            content=event['content'],
                            word_count=len(event['summary'].split()),
                            publish_time=event['last_updated'],
                            symbols=event["symbol"],  # due to bctc so it has only one symbol
                            url=event["url"],
                            language_id=1,
                        )
                    )
                    results = connection.execute(article_insert_st)
                    # results is a list of [article_id] -- or result.inserted_primary_key
                    idx = results.fetchall()[0][0]
                    event["id"] = idx
                    # insert into stock new table
                    sym_insert_st = self.symbol_table.insert().values(
                        symbol=event["symbol"],
                        article_id=event["article_id"],
                        title=event["title"],
                        language_id=1,
                        publish_time=event['last_updated'],
                        is_breaking_news=False,
                        is_crawled=True,
                    )
                    results = connection.execute(sym_insert_st)
                except Exception as e:
                    transaction.rollback()
                    logging.info('Inserting senseDB error. Rollbacked')
                    raise e
                transaction.commit()

    def get_current_id(self):
        try:
            with self.psql_engine.connect() as connection:
                article_select_st = (
                    select(self.article_table.c.article_id)
                    .where(self.article_table.c.article_id >= self.start_article_id)
                    .order_by(desc(self.article_table.c.article_id))
                    .limit(1)
                )
                results = connection.execute(article_select_st)
                # query return list of tuble -> [0][0]
                cur_id = results.fetchall()[0][0]
                return cur_id + 1
        except:
            logging.info(f"Empty DB for get current ID")
            return self.start_article_id

    def df_to_dicts(self, df):
        list_dict = []
        for row in df.iterrows():
            list_dict.append(row[1].to_dict())
        return list_dict

    def append(self, records):

        """
        Returns: dict of {symbol, article_url}
        """
        list_dict_record = self.df_to_dicts(records)
        symbols2article = {}
        for row in list_dict_record:
            print('Cur ID: ', self.current_id)
            title_slug = slugify(row['title'])
            row['article_id'] = self.current_id
            url = f'{self.sense_home_url}/{title_slug}-{self.current_id}?from={row["symbol"]}'
            row['url'] = url
            self.insert_sense_article_stock_new_from_dict(row)
            self.current_id += 1
            symbols2article[row['symbol']] = url
        context_key_name = f"CONTEXT_{self.company_type.upper()}_HAVE_BCTC"
        if self.quarter == 0:
            context_key_name = f"CONTEXT_{self.company_type.upper()}_HAVE_BCTC_NAM"

        Variable.set(key=context_key_name, value=symbols2article, serialize_json=True)
        return symbols2article
