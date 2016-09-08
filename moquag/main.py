from threading import Timer
from pymongo.errors import BulkWriteError
from pymongo.bulk import BulkOperationBuilder
from pymongo import MongoClient
import logging
import json
# imports not required by library


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


class BulkOperator(BulkOperationBuilder):

    def __init__(self, collection, ordered=True,
                 bypass_document_validation=False):
        """Initialize a new BulkOperator which extends BulkOperationBuilder.
        :Parameters:
          - `collection`: A :class:`~pymongo.collection.Collection` instance.
          - `ordered` (optional): If ``True`` all operations will be executed
            serially, in the order provided, and the entire execution will
            abort on the first error. If ``False`` operations will be executed
            in arbitrary order (possibly in parallel on the server), reporting
            any errors that occurred after attempting all operations. Defaults
            to ``True``.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**
        .. versionchanged:: 3.2
          Added bypass_document_validation support
        """
        super(BulkOperator, self).__init__(collection, ordered)
        self.find_count = 0
        self.insert_count = 0
        self.execute_count = 0
        self.total_ops = 0

    def total_ops(self):
        return self.find_count + self.insert_count + self.execute_count

    def find(self, *args):
        """Specify selection criteria for bulk operations.
        :Parameters:
          - `selector` (dict): the selection criteria for update
            and remove operations.
        :Returns:
          - A :class:`BulkWriteOperation` instance, used to add
            update and remove operations to this bulk operation.
        """
        self.find_count += 1
        self.total_ops += 1
        return super(BulkOperator, self).find(*args)

    def insert(self, *args):
        """Insert a single document.
        :Parameters:
          - `document` (dict): the document to insert
        .. seealso:: :ref:`writes-and-ids`
        """
        self.insert_count += 1
        self.total_ops += 1
        return super(BulkOperator, self).insert(*args)

    def execute(self, *args):
        """Execute all provided operations.
        :Parameters:
          - write_concern (optional): the write concern for this bulk
            execution.
        """
        self.execute_count += 1
        self.total_ops += 1
        return super(BulkOperator, self).execute(*args)

    def __str__(self):
        """The name of this :class:`Database`."""
        s = '<BulkOperator find: {}, insert: {}, execute:{}>'
        return s.format(self.find_count, self.insert_count, self.execute_count)


class Bulk:

    def __init__(self, db_name, conn, logger, ordered=True):
        self.__conn = conn
        self.__bulks = {}
        self.db_name = db_name
        self.logger = logger
        self.ordered = ordered

    def __next__(self):
        raise TypeError("'Bulk' object is not iterable")

    def __iter__(self):
        return self

    def __str__(self):
        """The name of this :class:`Database`."""
        return '<BulkOperatorInstanceForDatabase: {}>'.format(self.db_name)

    def __getattr__(self, collection):
        if collection not in self.__bulks:
            coll = self.__conn[self.db_name][collection]
            self.__bulks[collection] = BulkOperator(coll, self.ordered)
        return self.__bulks[collection]

    def __getitem__(self, name):
        return self.__getattr__(name)

    def execute(self):
        """Call this to flush the existing cached operations at db level
        """
        for coll, bulk in self.__bulks.items():
            try:
                self.logger.info('db: %s, coll: %s, ops: %s',
                                 self.db_name,
                                 coll,
                                 bulk)
                ops = bulk
                del self.__bulks[coll]
                result = ops.execute()
                self.logger.info(json.dumps(result))
            except BulkWriteError as bwe:
                self.logger.error(bwe.details)


class MongoQueryAggregator:

    def __init__(self, interval, mongodb_settings, logger=None):
        """Initialize a new MongoQueryAggregator.
        :Parameters:
          - `interval`: A :Integer:`seconds`.
          - `mongodb_settings` :dict: mongdb Settings.
          - `logger`:(optional) A :class: that is provided by logger settings
          Added bypass_document_validation support
        """
        self.__conn = None
        self.__dbs = {}
        self.interval = interval
        self.looper = None
        if logger:
            self.logger = logger
        else:
            logger = logging.getLogger()
        self.mongodb_settings = mongodb_settings

    def __str__(self):
        """Interval for batching:`seconds`."""
        return '<BatchingWindowInstance seconds: {}>'.format(self.interval)

    def __cnnction(self):
        if not self.__conn:
            self.mongodb_settings['connect'] = False
            self.__conn = MongoClient(**self.mongodb_settings)
        return self.__conn

    def __getattr__(self, db_name):
        if db_name not in self.__dbs:
            self.__dbs[db_name] = Bulk(db_name, self.__cnnction(), self.logger)
        return self.__dbs[db_name]

    def __getitem__(self, name):
        return self.__getattr__(name)

    def execute(self):
        """Call this to flush the existing cached operations
        """
        for db_name, bulk_ops in self.__dbs.items():
            bulk_ops.execute()

    def start(self):
        """starts a background thread to flush data to mongodb periodically as
        specified by buffer time.
        will not start a new thread if it is already running
        """
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
        """Flush existing data and stop periodic flushing thread
        """
        if self.looper:
            self.logger.info('########Stopping Loop########')
            self.looper.stop()
            self.looper = None
        else:
            self.logger.info('Not started')
            self.looper = None

    def restart(self):
        """Flush data if required and restart periodic flushing data
        """
        self.stop()
        self.start()
