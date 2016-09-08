import unittest
from pymongo import MongoClient
from moquag import MongoQueryAggregator
from time import sleep
from .settings import MONGO_DB_SETTINGS, logger


class TestBulk(unittest.TestCase):

    def setUp(self):
        self.buff_time = 1
        self.conn = MongoClient(**MONGO_DB_SETTINGS)
        self.conn.drop_database('testdb')
        self.b = MongoQueryAggregator(
            self.buff_time, MONGO_DB_SETTINGS, logger)
        self.b.start()

    def tearDown(self):
        self.conn.drop_database('testdb')
        self.conn.close()
        self.conn = None
        self.b.stop()

    def test_docs_insert(self):
        for i in range(1000000):
            self.b.testdb.testtable.insert({'key': 1})
        sleep(5 * self.buff_time)
        coll = self.conn['testdb']['testtable']
        count = coll.count()
        self.assertEqual(count, 1000000)
