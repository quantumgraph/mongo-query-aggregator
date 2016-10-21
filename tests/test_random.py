from pymongo import MongoClient
from moquag import MongoQueryAggregator
import thread
from time import sleep
from tests.settings import MONGO_DB_SETTINGS, logger
buff_time = 0.5
conn = MongoClient(**MONGO_DB_SETTINGS)
b = MongoQueryAggregator(
    buff_time, MONGO_DB_SETTINGS, logger)
b.start()


def write_to_db(threadName, delay, table):
    count = 0
    while True:
        sleep(delay)
        count += 1
        b.testdb[table].insert({'key': 1})


try:
    thread.start_new_thread(write_to_db, ("Thread-1", 0.001, 'profiles'))
    thread.start_new_thread(write_to_db, ("Thread-1", 0.001, 'profiles'))
    thread.start_new_thread(write_to_db, ("Thread-1", 0.001, 'profiles'))
    thread.start_new_thread(write_to_db, ("Thread-1", 0.001, 'events'))
    thread.start_new_thread(write_to_db, ("Thread-1", 0.001, 'events'))
    thread.start_new_thread(write_to_db, ("Thread-1", 0.001, 'events'))
    thread.start_new_thread(write_to_db, ("Thread-1", 0.001, 'events'))
    thread.start_new_thread(write_to_db, ("Thread-1", 0.001, 'events'))
    thread.start_new_thread(write_to_db, ("Thread-1", 0.001, 'user-details'))
    thread.start_new_thread(write_to_db, ("Thread-10", 0.001, 'user-details'))
    thread.start_new_thread(write_to_db, ("Thread-11", 0.001, 'profiles'))
    thread.start_new_thread(write_to_db, ("Thread-12", 0.001, 'profiles'))
    thread.start_new_thread(write_to_db, ("Thread-13", 0.001, 'profiles'))
    thread.start_new_thread(write_to_db, ("Thread-14", 0.001, 'events'))
    thread.start_new_thread(write_to_db, ("Thread-15", 0.001, 'events'))
    thread.start_new_thread(write_to_db, ("Thread-16", 0.001, 'events'))
    thread.start_new_thread(write_to_db, ("Thread-17", 0.001, 'events'))
    thread.start_new_thread(write_to_db, ("Thread-18", 0.001, 'events'))
    thread.start_new_thread(write_to_db, ("Thread-19", 0.001, 'user-details'))
    thread.start_new_thread(write_to_db, ("Thread-20", 0.001, 'user-details'))
    thread.start_new_thread(write_to_db, ("Thread-21", 0.001, 'user-details'))
    thread.start_new_thread(write_to_db, ("Thread-22", 0.001, 'test-details'))
except:
    print "Error: unable to start thread"
