import unittest
import time
from pymongo import MongoClient
from moquag import MongoQueryAggregator
from time import sleep
from .settings import MONGO_DB_SETTINGS, logger
from collections import Counter


class TestBulk(unittest.TestCase):

    def setUp(self):
        self.conn = MongoClient(**MONGO_DB_SETTINGS)
        self.conn.drop_database('testdb1')
        self.conn.drop_database('testdb2')
        self.conn.drop_database('testdb3')

    def test_1(self):
        '''updating one doc waiting for 0.1 sec flushing it and checking data'''
        mongo_agg = MongoQueryAggregator(MONGO_DB_SETTINGS, 0.1, 10)
        docs = [
            {'name': 'User1', 'id': 1},
            {'name': 'User2', 'id': 2}
        ]
        self.conn['testdb1'].profiles.insert(docs)

        mongo_agg.testdb1.profiles.find({'id': 1}).upsert().update({'$set': {'id2': 1}})
        time.sleep(0.1)
        mongo_agg.testdb1.profiles.find({'id': 2}).upsert().update({'$set': {'id2': 2}})
        self.assertEqual(self.conn['testdb1'].profiles.count(), 2)
        self.assertEqual(self.conn['testdb1'].profiles.count({'id2': 1}), 1)
        for doc in self.conn['testdb1'].profiles.find({'id': 1}, {'id2': 1, '_id': 0}):
            self.assertEqual(doc['id2'], 1)


    def test_2(self):
        '''inserting data using mongo_agg upsert'''
        mongo_agg = MongoQueryAggregator(MONGO_DB_SETTINGS, 0.1, 5)
        docs = [
            {'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}, {'id': 5}, {'id': 6}
        ]
        for doc in docs:
            mongo_agg.testdb1.profiles.find(doc).upsert().update({'$set': doc})
        # while inserting 6 records, 6th record will flush first 5 to db
        data = self.conn['testdb1'].profiles.find({}, {'_id': 0, 'id': 1}).sort([('id', 1)])
        mongo_docs = []
        self.assertEqual(data.count(), 5)

        for doc in data:
            mongo_docs.append(doc)
        self.assertListEqual(mongo_docs, docs[:5]) # checking first five in docs

    def test_3(self):
        '''inserting 6 documents and updating it using uosert in 2 diff dbs
        and checking values in both dbs'''
        mongo_agg = MongoQueryAggregator(MONGO_DB_SETTINGS, 0.1, 5)
        docs = [
            {'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}, {'id': 5}, {'id': 6}
        ]
        self.conn['testdb1'].profiles.insert(docs[:3])
        self.conn['testdb2'].profiles.insert(docs[3:])
        mongo_agg.testdb1.profiles.find({}).upsert().update({'$set': {'status': 'updated'}})
        mongo_agg.testdb2.profiles.find({}).upsert().update({'$set': {'status': 'updated'}})
        time.sleep(0.1)
        # query just to flush older queries
        mongo_agg.testdb2.profiles.find({'css': 'sacas'}).update({'$set': docs[0]})

        docs_in_db = []
        aggregators_expected_results = {
            ('testdb1', 'profiles'): Counter({'nModified': 3, 'nMatched': 3}),
            ('testdb2', 'profiles'): Counter({'nModified': 3, 'nMatched': 3})
        }
        data = self.conn.testdb1.profiles.find()
        output_data = []
        for doc in data:
            output_data.append(doc)

        data = self.conn.testdb2.profiles.find()
        for doc in data:
            output_data.append(doc)

        self.assertEqual(aggregators_expected_results, mongo_agg.get_results())
        for doc in docs:
            doc['status'] = 'updated'
        self.assertListEqual(output_data, docs)
