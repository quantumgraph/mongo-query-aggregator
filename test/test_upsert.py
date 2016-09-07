import unittest
from pymongo import MongoClient
from moquag.main import BatchingWindow
import logging
from time import sleep
logger = logging.getLogger()
MONGO_DB_SETTINGS = {
    'port': 27000
}


class TestUpsertDoc(unittest.TestCase):

    def setUp(self):
        self.buff_time = 1
        self.conn = MongoClient(**MONGO_DB_SETTINGS)
        self.b = BatchingWindow(self.buff_time, MONGO_DB_SETTINGS, logger)
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
        sleep(5*self.buff_time)
        self.b.testdb.testtable.find({'key': 1}).upsert().update({'$set':
                                                                  {'key': 6}})
        self.b.testdb.testtable.find({'key': 2}).upsert().update({'$set':
                                                                  {'key': 7}})
        self.b.testdb.testtable.find({'key': 3}).upsert().update({'$set':
                                                                  {'key': 8}})
        sleep(5*self.buff_time)
        coll = self.conn['testdb']['testtable']
        for i in coll.find():
            self.assertEqual(i['key'] in [6, 7, 8], True)
