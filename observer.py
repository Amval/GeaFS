from pymongo import MongoClient
from pymongo.cursor import _QUERY_OPTIONS
from pymongo.errors import AutoReconnect
import time

# Tailable cursor options.
_TAIL_OPTS = {'tailable': True, 'await_data': True}

# Time to wait for data or connection.
_SLEEP = 10

if __name__ == '__main__':
    db = MongoClient().local
    while True:
        query = {'filename': {'$regex':"^\\geafiles\\.*"}}  # Replace with your own query.
        cursor = db.oplog.rs.find(query, **_TAIL_OPTS)

        cursor.add_option(_QUERY_OPTIONS['oplog_replay'])

        try:
            while cursor.alive:
                try:
                    doc = cursor.next()
                    print(doc)
                except (AutoReconnect, StopIteration):
                    time.sleep(_SLEEP)
        finally:
            cursor.close()