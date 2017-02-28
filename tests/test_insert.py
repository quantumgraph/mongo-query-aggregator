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
        '''inserting 2 document in interval of 0.1 sec'''
        mongo_agg = MongoQueryAggregator(MONGO_DB_SETTINGS, 0.1, 10)
        docs = [
            {'name': 'User1', 'id': 1},
            {'name': 'User2', 'id': 2}
        ]
        x = mongo_agg.testdb1.profiles.insert(docs[0])
        data = self.conn['testdb1'].profiles.find()

        # added one doc to aggregator, it should not be inserted to DB yet
        # as time interval 0.1 sec is not passed and doc limit of 10 is not crossed
        self.assertEqual(self.conn['testdb1'].profiles.count(), 0)
        time.sleep(0.1)
        mongo_agg.testdb1.profiles.insert(docs[1])
        # docs[1] is inserted after 0.1 sec so it should flush older data
        # so docs[0] should be inserted to mongo
        data = self.conn['testdb1'].profiles.find()

        self.assertEqual(data.count(), 1)
        for doc in data:
            self.assertEqual(doc, docs[0])

    def test_2(self):
        '''inserting 6 documents with max_ops_limit=5
        so first five docs shoulld be present in mongodb'''
        mongo_agg = MongoQueryAggregator(MONGO_DB_SETTINGS, 1, 5)
        docs = [
            {'name': 'User1', 'id': 1},
            {'name': 'User2', 'id': 2},
            {'name': 'User3', 'id': 3},
            {'name': 'User4', 'id': 4},
            {'name': 'User5', 'id': 5},
            {'name': 'User6', 'id': 6}
        ]
        for doc in docs:
            mongo_agg.testdb1.profiles.insert(doc)
        # while inserting 6 records, 6th record will flush first 5 to db
        data = self.conn['testdb1'].profiles.find().sort([('id', 1)])
        mongo_docs = []
        self.assertEqual(data.count(), 5)

        for doc in data:
            mongo_docs.append(doc)
        self.assertListEqual(mongo_docs, docs[:5]) # checking first five in docs

    def test_3(self):
        '''inserting 6 documents with max_ops_limit=5
        so first five docs should be present in mongodb
        here inserting to multiple dbs'''
        mongo_agg = MongoQueryAggregator(MONGO_DB_SETTINGS, 1, 5)
        docs = [
            {'name': 'User1', 'id': 1},
            {'name': 'User2', 'id': 2},
            {'name': 'User3', 'id': 3},
            {'name': 'User4', 'id': 4},
            {'name': 'User5', 'id': 5},
            {'name': 'User6', 'id': 6}
        ]
        for doc in docs[:6]:
            # inserting first 6 records to db testdb1
            mongo_agg.testdb1.profiles.insert(doc)
        mongo_agg.testdb2.profiles.insert({'name': 'User1', 'id': 1})
        # while inserting 6 records, 6th record will flush first 5 to db
        data = self.conn['testdb1'].profiles.find().sort([('id', 1)])

        docs_in_db = []
        for doc in data:
            docs_in_db.append(doc)

        self.assertListEqual(docs_in_db, docs[:5])
        aggregators_expected_results = {
            ('testdb1', 'profiles'): Counter({'nInserted': 5}),
            ('testdb2', 'profiles'): Counter()
        }
        aggregators_results = mongo_agg.get_results()
        self.assertEqual(aggregators_expected_results,aggregators_results)

    def test_4(self):
        '''inserting to multiple data to multiple dbs
        and checking mongo data and aggregators resuts'''
        mongo_agg = MongoQueryAggregator(MONGO_DB_SETTINGS, 0.1, 5)
        dbs_to_data = {
            'testdb1': [
                {'name': 'User2', 'id': 1}, {'name': 'User2', 'id': 2}
            ],
            'testdb2': [
                {'name': 'User3', 'id': 3}
            ],
            'testdb3': [
                {'name': 'User5', 'id': 5},
                {'name': 'User6', 'id': 6},
                {'name': 'User7', 'id': 8}
            ]
        }
        for db_name in dbs_to_data:
            for doc in dbs_to_data[db_name]:
                mongo_agg[db_name].profiles.insert(doc)
        time.sleep(0.1)
        mongo_agg.testdb1.profiles.insert({'key': 1}) # dummy data to flush older data
        
        projection = {'name': 1, 'id': 1, '_id': 0}
        for db_name in dbs_to_data:
            data = self.conn[db_name].profiles.find().sort([('id', 1)])
            self.assertEqual(data.count(), len(dbs_to_data[db_name]))
            docs_in_db = []
            for doc in data:
                docs_in_db.append(doc)
            self.assertListEqual(docs_in_db, dbs_to_data[db_name])
        aggregators_expected_results = {
            ('testdb1', 'profiles'): Counter({'nInserted': 2}),
            ('testdb3', 'profiles'): Counter({'nInserted': 3}),
            ('testdb2', 'profiles'): Counter({'nInserted': 1})
        }
        aggregators_results = mongo_agg.get_results()
        self.assertEqual(aggregators_expected_results, aggregators_results)
