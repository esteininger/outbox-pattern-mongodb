from pymongo import MongoClient
from config import mongo_uri, database, outbox_collection

# mongo connection data
client = MongoClient(mongo_uri)
db = client[database]
outbox_collection = db[outbox_collection]

def listen_for_large_orders():
    # Change stream pipeline
    pipeline = [
        {'$match': {'fullDocument.amount': {'$gte':1000}}}
    ]
    try:
        print('listening...')
        for document in outbox_collection.watch(pipeline=pipeline):
            print(f"\n=== LARGE AMOUNT {document['fullDocument']['actionType']} ===\n")
            print(document['fullDocument'])

    except KeyboardInterrupt:
        keyboard_shutdown()

if __name__ == '__main__':
    listen_for_large_orders()
