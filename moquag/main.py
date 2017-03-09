from pymongo.errors import BulkWriteError
from pymongo.bulk import BulkOperationBuilder
import traceback
from pymongo import MongoClient
from collections import Counter
from time import time
import sys

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
            ret = super(BulkOperator, self).execute(*args)
            result_counter = {}
            for key in ret:
                if key in ['writeConcernErrors', 'writeErrors']:
                    if len(ret[key]) > 0:
                        result_counter[key] = len(ret[key])
                        sys.stderr.write('{}:\n\t{}'.format(key, ret[key]))
                elif key != 'upserted':
                    result_counter[key] = ret[key]
            return Counter(result_counter)
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

    def __init__(self, conn, db_name, results, max_ops_limit, ordered=True):
        self.__conn = conn
        self.__bulks = {}
        self.db_name = db_name
        self.ordered = ordered
        self.max_ops_limit = max_ops_limit
        self.results = results

    def __next__(self):
        raise TypeError("'Bulk' object is not iterable")

    def __str__(self):
        """The name of this :class:`Database`."""
        return '{"BulkOperatorInstanceForDatabase": "{}"'.format(self.db_name)

    def __repr__(self):
        """Interval for batching:`seconds`."""
        return self.__str__()

    def update_results(self, collection, curr_result):
        results_key = (self.db_name, collection)
        self.results.setdefault(results_key, Counter())
        self.results[results_key] += curr_result

    def __getattr__(self, collection):
        if collection not in self.__bulks:
            coll = self.__conn[self.db_name][collection]
            self.__bulks[collection] = BulkOperator(coll, self.ordered)
        elif self.__bulks[collection].total_ops >= self.max_ops_limit:
            curr_result = Counter(self.__bulks[collection].execute())
            self.update_results(collection, curr_result)
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
                bulkOp = self.__bulks[coll]
                curr_result = Counter(bulkOp.execute())
                self.update_results(coll, curr_result)
            except BulkWriteError as bwe:
                sys.stderr.write(str(bwe.details))


class MongoQueryAggregator:

    def __init__(self, mongodb_settings, interval, max_ops_limit):
        """Initialize a new MongoQueryAggregator.
        :Parameters:
          - `interval`: A :Integer:`seconds`.
          - `mongodb_settings` :dict: mongdb Settings.
          - `max_ops_limit`: A :Integer: max number of operation a Bulk can hold for a collection
            crossing this limit it will execute already cached operations
        """
        self.__conn = None
        self.__dbs = {}
        self.__interval = interval
        self.mongodb_settings = mongodb_settings
        self.max_ops_limit = max_ops_limit
        self.last_execution_time = time()
        self.results = {}

    def __str__(self):
        """Interval for batching:`seconds`."""
        return '{"seconds": {}}'.format(self.__interval)

    def __repr__(self):
        """Interval for batching:`seconds`."""
        return self.__str__()

    def __connection(self):
        if not self.__conn:
            self.mongodb_settings['connect'] = False
            self.__conn = MongoClient(**self.mongodb_settings)
        return self.__conn

    def __getattr__(self, db_name):
        if self.__interval + self.last_execution_time <= time():
            self.execute()

        if db_name not in self.__dbs:
            self.__dbs[db_name] = Bulk(self.__connection(), db_name, self.results, self.max_ops_limit)
        return self.__dbs[db_name]

    def __getitem__(self, name):
        return self.__getattr__(name)

    def execute(self):
        """Call this to flush the existing cached operations
        """
        for db_name in list(self.__dbs):
            try:
                self.__dbs[db_name].execute()
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                traceback_log = ''.join(line for line in lines)
                sys.stderr.write('For DB [ {} ]\n\t'.format(db_name, traceback_log))
        self.__dbs = {}
        self.last_execution_time = time()

    def __del__(self):
        '''execute all pending queries on deleting instance of MongoQueryAggregator'''
        self.execute()

    def get_results(self):
        return self.results

    def get_and_reset_results(self):
        '''this function returns current results and resets results'''
        results = self.results
        self.results = {}
        for db_name, bulkOp in self.__dbs.iteritems():
            bulkOp.results = self.results
        return results
