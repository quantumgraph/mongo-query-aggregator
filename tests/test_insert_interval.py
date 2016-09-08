import unittest
from pymongo import MongoClient
from moquag import MongoQueryAggregator
import logging
from time import sleep
logger = logging.getLogger()
MONGO_DB_SETTINGS = {
    'port': 27000
}


class TestDocInsert10ms(unittest.TestCase):

    def setUp(self):
        self.buff_time = 0.01
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
        self.b.testdb.testtable.insert({'key': 1})
        self.b.testdb.testtable.insert({'key': 2})
        self.b.testdb.testtable.insert({'key': 3})
        sleep(2 * self.buff_time)
        coll = self.conn['testdb']['testtable']
        for i in coll.find():
            self.assertEqual(i['key'] in [1, 2, 3], True)


class TestDocInsert100ms(unittest.TestCase):

    def setUp(self):
        self.buff_time = 0.1
        self.conn = MongoClient(**MONGO_DB_SETTINGS)
        self.b = MongoQueryAggregator(
            self.buff_time, MONGO_DB_SETTINGS, logger)
        self.b.start()

    def tearDown(self):
        self.conn.drop_database('testdb')
        self.conn.close()
        self.conn = None
        self.b.stop()

    def test_docs_insert(self):
        self.b.testdb.testtable.insert({'key': 1})
        self.b.testdb.testtable.insert({'key': 2})
        self.b.testdb.testtable.insert({'key': 3})
        sleep(2 * self.buff_time)
        coll = self.conn['testdb']['testtable']
        for i in coll.find():
            self.assertEqual(i['key'] in [1, 2, 3], True)


class TestDocInsert5s(unittest.TestCase):

    def setUp(self):
        self.buff_time = 5
        self.conn = MongoClient(**MONGO_DB_SETTINGS)
        self.b = MongoQueryAggregator(
            self.buff_time, MONGO_DB_SETTINGS, logger)
        self.b.start()

    def tearDown(self):
        self.conn.drop_database('testdb')
        self.conn.close()
        self.conn = None
        self.b.stop()

    def test_docs_insert(self):
        self.b.testdb.testtable.insert({'key': 1})
        self.b.testdb.testtable.insert({'key': 2})
        self.b.testdb.testtable.insert({'key': 3})
        sleep(2 * self.buff_time)
        coll = self.conn['testdb']['testtable']
        for i in coll.find():
            self.assertEqual(i['key'] in [1, 2, 3], True)
