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
        '''inserting 2 document in interval of 0.1 sec
        and updating same 2 documents'''
        mongo_agg = MongoQueryAggregator(MONGO_DB_SETTINGS, 0.1, 10)
        docs = [
            {'name': 'User1', 'id': 1},
            {'name': 'User2', 'id': 2}
        ]

        #------ insert data to update --------
        mongo_agg.testdb1.profiles.insert(docs[0])
        mongo_agg.testdb1.profiles.insert(docs[1])
        #-------------------------------------
        mongo_agg.testdb1.profiles.find({'id': 1}).upsert().update({'$set': {'updated': True}})
        mongo_agg.testdb1.profiles.find({'id': 2}).upsert().update({'$set': {'updated': True}})
        self.assertEqual(self.conn['testdb1'].profiles.count(), 0)
        time.sleep(0.1)
        mongo_agg.testdb1.profiles.insert({'dummy': 1}) # just to flush old data

        data = self.conn['testdb1'].profiles.find()
        self.assertEqual(self.conn['testdb1'].profiles.count(), 2)
        data_in_dbs = []
        for doc in data:
            data_in_dbs.append(doc)

        for doc in docs:
            doc['updated'] = True
        self.assertListEqual(docs, data_in_dbs)
        aggregators_expected_results = {
            ('testdb1', 'profiles'): Counter({'nModified': 2, 'nMatched': 2, 'nInserted': 2})
        }
        self.assertEqual(aggregators_expected_results, mongo_agg.get_results())

    def test_2(self):
        '''Inserting/updating multiple documents to multiple dbs db doing operation greater than
        max_ops_limit and checking the dbs
        '''
        mongo_agg = MongoQueryAggregator(MONGO_DB_SETTINGS, 0.1, 5)
        dbs_to_data_to_insert = {
            'testdb1': [ {'id': 1}, {'id': 2} ],
            'testdb2': [ {'id': 3} ],
            'testdb3': [ {'id': 5}, {'id': 6}, {'id': 8} ]
        }
        db_to_data_to_update = {
            'testdb1': [ ({'id': 1}, {'name': 'new1'}) ],
            'testdb2': [ ({'id': 100}, {'name': 'new100'}) ], # will insert
            'testdb3': [
                ({'id': 5}, {'name': 'new5'}),
                ({'id': 6}, {'name': 'new6'})
            ]
        }
        for db_name, docs in dbs_to_data_to_insert.iteritems():
            for doc in docs:
                mongo_agg[db_name].profiles.insert(doc)

        for db_name, data in db_to_data_to_update.iteritems():
            for search_query, update_query in data:
                mongo_agg[db_name].profiles.find(search_query).upsert().update({'$set': update_query})
        time.sleep(0.1)
        mongo_agg.testdb1.profiles.insert({'dummy': 1}) # just to flush old data

        # expected data on dbs
        expected_data =  {
            'testdb1': [ {'id': 1, 'name': 'new1'}, {'id': 2} ],
            'testdb2': [ {'id': 3}, {'id': 100, 'name': 'new100'} ],
            'testdb3': [ {'id': 5, 'name': 'new5'}, {'id': 6, 'name': 'new6'}, {'id': 8} ]
        }
        for db in expected_data:
            data = self.conn[db].profiles.find({}, {'name': 1, 'id': 1, '_id': 0})
            # db  data count check
            self.assertEqual(data.count(), len(expected_data[db]))
            data_in_db = []
            for doc in data:
                data_in_db.append(doc)
            self.assertListEqual(data_in_db, expected_data[db])
        aggregators_expected_results = {
            ('testdb1', 'profiles'): Counter({'nInserted': 2, 'nModified': 1, 'nMatched': 1}),
            ('testdb3', 'profiles'): Counter({'nInserted': 3, 'nModified': 2, 'nMatched': 2}),
            ('testdb2', 'profiles'): Counter({'nUpserted': 1, 'nInserted': 1})
        }
        self.assertEqual(mongo_agg.get_results(), aggregators_expected_results)

    def test_3(self):
        '''Inserting/updating multiple documents to multiple dbs and multiple collections
        '''
        mongo_agg = MongoQueryAggregator(MONGO_DB_SETTINGS, 0.1, 5)
        dbs_to_data_to_insert = {
            ('testdb1', 'profiles'): [ {'id': 1}, {'id': 2} ],
            ('testdb2', 'events'): [ {'id': 3} ],
            ('testdb3', 'users_details'): [ {'id': 5}, {'id': 6}, {'id': 8} ]
        }
        db_to_data_to_update = {
            ('testdb1', 'profiles'): [ ({'id': 1}, {'name': 'new1'}) ],
            ('testdb2', 'events'): [ ({'id': 100}, {'name': 'new100'}) ], # will insert
            ('testdb3', 'users_details'): [
                ({'id': 5}, {'name': 'new5'}),
                ({'id': 6}, {'name': 'new6'})
            ]
        }
        for (db_name, collection_name), docs in dbs_to_data_to_insert.iteritems():
            for doc in docs:
                mongo_agg[db_name][collection_name].insert(doc)

        for (db_name, collection_name), data in db_to_data_to_update.iteritems():
            for search_query, update_query in data:
                mongo_agg[db_name][collection_name].find(search_query).upsert().update({'$set': update_query})
        time.sleep(0.1)
        mongo_agg.testdb1.profiles.insert({'dummy': 1}) # just to flush old data

        # expected data in dbs
        expected_data =  {
            ('testdb1', 'profiles'): [ {'id': 1, 'name': 'new1'}, {'id': 2} ],
            ('testdb2', 'events'): [ {'id': 3}, {'id': 100, 'name': 'new100'} ],
            ('testdb3', 'users_details'): [
                {'id': 5, 'name': 'new5'}, {'id': 6, 'name': 'new6'}, {'id': 8}
            ]
        }
        for (db_name, collection_name) in expected_data:
            data = self.conn[db_name][collection_name].find({}, {'name': 1, 'id': 1, '_id': 0})
            # db  data count check
            self.assertEqual(data.count(), len(expected_data[(db_name, collection_name)]))
            data_in_db = []
            for doc in data:
                data_in_db.append(doc)
            self.assertListEqual(data_in_db, expected_data[(db_name, collection_name)])
        aggregators_expected_results = {
            ('testdb1', 'profiles'): Counter({'nInserted': 2, 'nModified': 1, 'nMatched': 1}),
            ('testdb3', 'users_details'): Counter({'nInserted': 3, 'nModified': 2, 'nMatched': 2}),
            ('testdb2', 'events'): Counter({'nUpserted': 1, 'nInserted': 1})
        }
        self.assertEqual(mongo_agg.get_results(), aggregators_expected_results)
