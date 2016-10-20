from threading import Timer
from pymongo.errors import BulkWriteError
from pymongo.bulk import BulkOperationBuilder
from pymongo import MongoClient
import logging
moquag_logger = logging.getLogger('moquag')
# imports not required by library


class RepeatedTimer(object):

    def __init__(self, logger, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.logger = logger or moquag_logger
        self.is_running = False
        self.start()
        self.count = 0

    def _run(self):
        self.count += 1
        self.is_running = False
        self.start()
        if self.count % 120 == 0:
            self.logger.debug('RunningThread loop: %s', self.count)
            if self.count > 32768:
                self.count = 0
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

    def __init__(self, collection, ordered=False,
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
        if self.total_ops > 0:
            return super(BulkOperator, self).execute(*args)
        else:
            return {'ops': 'No ops found'}

    def __str__(self):
        """The name of this :class:`Database`."""
        s = '"BOFind": {}, "BOInsert": {}, "BOExecute":{}'
        return s.format(self.find_count, self.insert_count, self.execute_count)

    def __repr__(self):
        """The name of this :class:`Database`."""
        return self.__str__()


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
        return '{"BulkOperatorInstanceForDatabase": "{}"'.format(self.db_name)

    def __repr__(self):
        """Interval for batching:`seconds`."""
        return self.__str__()

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
        for coll in list(self.__bulks):
            try:
                self.logger.info('db: %s, coll: %s, ops: %s',
                                 self.db_name,
                                 coll,
                                 self.__bulks[coll])
                ops = self.__bulks[coll]
                del self.__bulks[coll]
                result = ops.execute()
                self.logger.info(result)
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
        self.__interval = interval
        self.looper = None
        self.is_executing = False
        self.logger = logger or moquag_logger
        self.mongodb_settings = mongodb_settings

    def __str__(self):
        """Interval for batching:`seconds`."""
        return '{"seconds": {}}'.format(self.__interval)

    def __repr__(self):
        """Interval for batching:`seconds`."""
        return self.__str__()

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
        if self.is_executing:
            self.logger.info(
                'Execution is already in progress by different thread'
            )
            return
        for db_name, bulk_ops in self.__dbs.items():
            bulk_ops.execute()

    def start(self):
        """starts a background thread to flush data to mongodb periodically as
        specified by buffer time.
        will not start a new thread if it is already running
        """
        if self.looper:
            self.logger.info('Already started')
            self.logger.info('looping every %s seconds', self.__interval)
        else:
            self.looper = RepeatedTimer(self.logger,
                                        self.__interval,
                                        self.execute)
            self.looper.start()
            self.logger.info('########Starting Loop########')
            self.logger.info('looping every %s seconds', self.__interval)

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
