import logging
import pprint

from pymysql.cursors import DictCursor

from pymysql import OperationalError
from pymysql.constants.CR import CR_SERVER_GONE_ERROR,  CR_SERVER_LOST, CR_CONNECTION_ERROR
from twisted.internet import defer
from twisted.enterprise import adbapi

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class MySQLPipeline(object):  #
    """
    Defaults:
    MYSQL_HOST = 'localhost'
    MYSQL_PORT = 3306
    MYSQL_USER = None
    MYSQL_PASSWORD = ''
    MYSQL_DB = None
    MYSQL_TABLE = None
    MYSQL_UPSERT = False
    MYSQL_RETRIES = 3
    MYSQL_CLOSE_ON_ERROR = True
    MYSQL_CHARSET = 'utf8'
    Pipeline:
    ITEM_PIPELINES = {
       'scrapy_mysql_pipeline.MySQLPipeline': 300,
    }
    """
    stats_name = 'mysql_pipeline'

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        self.stats = crawler.stats
        self.settings = crawler.settings
        db_args = {
            'host': self.settings.get('MYSQL_HOST', 'localhost'),
            'port': self.settings.get('MYSQL_PORT', 3306),
            'user': self.settings.get('MYSQL_USER', None),
            'password': self.settings.get('MYSQL_PASSWORD', ''),
            'db': self.settings.get('MYSQL_DB', None),
            'charset': self.settings.get('MYSQL_CHARSET', 'utf8'),
            'cursorclass': DictCursor,
            'cp_reconnect': True,
        }
        self.retries = self.settings.get('MYSQL_RETRIES', 3)
        self.close_on_error = self.settings.get('MYSQL_CLOSE_ON_ERROR', True)
        self.upsert = self.settings.get('MYSQL_UPSERT', False)
        self.table = self.settings.get('MYSQL_TABLE', None)
        self.column_list = self.settings.get('MYSQL_COLUMN_LIST', [])
        self.db = adbapi.ConnectionPool('pymysql', **db_args)

    def close_spider(self, spider):
        self.db.close()

    @staticmethod
    def preprocess_item(item):
        return item

    def postprocess_item(self, *args):
        pass

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        retries = self.retries
        status = False
        while retries:
            try:
                item = self.preprocess_item(item)
                yield self.db.runInteraction(self._process_item, item)
            except OperationalError as e:
                if e.args[0] in (
                        CR_SERVER_GONE_ERROR,
                        CR_SERVER_LOST,
                        CR_CONNECTION_ERROR,
                ):
                    retries -= 1
                    logger.info('%s %s attempts to reconnect left', e, retries)
                    self.stats.inc_value('{}/reconnects'.format(self.stats_name))
                    continue
                logger.exception('%s', pprint.pformat(item))
                self.stats.inc_value('{}/errors'.format(self.stats_name))
            except Exception:
                logger.exception('%s', pprint.pformat(item))
                self.stats.inc_value('{}/errors'.format(self.stats_name))
            else:
                status = True  # executed without errors
            break
        else:
            if self.close_on_error:  # Close spider if connection error happened and MYSQL_CLOSE_ON_ERROR = True
                spider.crawler.engine.close_spider(spider, '{}_fatal_error'.format(self.stats_name))
        self.postprocess_item(item, status)
        yield item

    def _generate_sql(self, data):
        columns = lambda d: ', '.join(['`{}`'.format(k) for k in d if k in self.column_list])
        values = lambda d: [d[k] for k in d if k in self.column_list]
        placeholders = lambda d: ', '.join(['%s'] * len([k for k in d if k in self.column_list]))
        if self.upsert:
            sql_template = 'INSERT INTO `{}` ( {} ) VALUES ( {} ) ON DUPLICATE KEY UPDATE {}'
            on_duplicate_placeholders = lambda d: ', '.join(['`{}` = %s'.format(k) for k in d])
            return (
                sql_template.format(
                    self.table, columns(data),
                    placeholders(data), on_duplicate_placeholders(data)
                ),
                values(data) + values(data)
            )
        else:
            sql_template = 'INSERT INTO `{}` ( {} ) VALUES ( {} )'
            return (
                sql_template.format(self.table, columns(data), placeholders(data)),
                values(data)
            )

    def _process_item(self, tx, row):
        sql, data = self._generate_sql(row)
        try:
            tx.execute(sql, data)
        except Exception:
            logger.error("SQL: %s", sql)
            raise
        self.stats.inc_value('{}/saved'.format(self.stats_name))