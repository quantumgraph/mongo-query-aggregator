from threading import Timer
from pymongo.errors import BulkWriteError
from pymongo import MongoClient
# imports not required by library
import logging
from settings import (
    init_logging,
    MONGO_DB_SETTINGS,
    BUFFER_TIME
)


class RepeatedTimer(object):

    def __init__(self, logger, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.logger = logger
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.logger.debug('running thread')
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


class Bulk:

    def __init__(self, db_name, conn, logger):
        self.__conn = conn
        self.__bulks = {}
        self.db_name = db_name
        self.logger = logger

    def __str__(self):
        """The name of this :class:`Database`."""
        return '<BulkOperatorInstanceForDatabase: {}>'.format(self.db_name)

    def __getattr__(self, collection):
        if collection not in self.__bulks:
            coll = self.__conn[self.db_name][collection]
            self.__bulks[collection] = coll.initialize_unordered_bulk_op()
        return self.__bulks[collection]

    def __getitem__(self, name):
        return self.__getattr__(name)

    def execute(self, bulk_dict):
        for db_name, bulk in self.__bulks():
            try:
                result = bulk.execute()
                logger.info(result)
            except BulkWriteError as bwe:
                logger.error(bwe.details)


class BatchingWindow:

    def __init__(self, interval, logger):
        self.__conn = None
        self.__dbs = {}
        self.interval = interval
        self.looper = None
        self.logger = logger

    def __str__(self):
        """Interval for batchine:`seconds`."""
        return '<BatchingWindowInstance seconds: {}>'.format(self.interval)

    def conn(self):
        if not self.__conn:
            MONGO_DB_SETTINGS['connect'] = False
            self.__conn = MongoClient(**MONGO_DB_SETTINGS)
        return self.__conn

    def __getattr__(self, db_name):
        if db_name not in self.__dbs:
            self.__dbs[db_name] = Bulk(db_name, self.conn(), self.logger)
        return self.__dbs[db_name]

    def __getitem__(self, name):
        return self.__getattr__(name)

    def execute(self):
        for db_name, bulks in self.__dbs:
            ops = bulks
            del self.bulks[db_name]
            for bulk in ops:
                bulk.execute()

    def start(self):
        if self.looper:
            self.logger.info('Already started')
            self.logger.info('looping every %s seconds', self.interval)
        else:
            self.looper = RepeatedTimer(self.logger,
                                        self.interval,
                                        self.execute)
            self.looper.start()
            self.logger.info('########Starting Loop########')
            self.logger.info('looping every %s seconds', self.interval)

    def stop(self):
        if self.looper:
            self.logger.info('########Stopping Loop########')
            self.looper.stop()
            self.looper = None
        else:
            logger.info('Not started')
            self.looper = None

    def restart(self):
        self.stop()
        self.start()


logger = logging.getLogger('batcher')
init_logging()

if __name__ == '__main__':
    print('hello')
    logger.info('starting batcher')
    b = BatchingWindow(BUFFER_TIME, logger)
    print(b)
    print(b.ravi)
    b.start()
    b.stop()
