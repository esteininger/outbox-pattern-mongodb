from pymongo import MongoClient
from pymongo.collection import ReturnDocument
from config import mongo_uri, database, outbox_collection, orders_collection
import random
from faker import Faker
import uuid
import time

fake = Faker()
NUM_RECORDS_TO_GENERATE = 10

# mongo connection data
client = MongoClient(mongo_uri)
db = client[database]


def insert(session):
    order = {
        "email": fake.email(),
        "amount": random.randint(0, 5000),
        "insert_timestamp": time.time() * 1000,
        "bank": {
            "account": fake.bban(),
            "currency": fake.currency_code()
        }
    }
    # insert to orders collection
    db[orders_collection].insert_one(order, session=session)
    # copy in trx to outbox
    copy_to_outbox(payload=order, action_type='insertOrder', session=session)
    print(f'inserted: \n {order}')


def update(session):
    # change amount for every amount less than 500
    op = db[orders_collection].find_one_and_update(
        {'amount': {'$lte': 2500}},
        {'$inc': {'amount': random.randint(0, 500)}},
        upsert=False,
        return_document=ReturnDocument.AFTER
    )
    # if it found any
    if op is not None:
        # copy in trx to outbox
        copy_to_outbox(payload=op, action_type='updateOrder', session=session)
        print(f'updated: \n {op}')


def delete(session):
    # delete orders greater than 4000
    op = db[orders_collection].find_one_and_delete(
        {'amount': {'$lte': 2000}}
    )
    # if it found any
    if op is not None:
        # copy in trx to outbox
        copy_to_outbox(payload=op, action_type='deleteOrder', session=session)
        print(f'deleted: \n {op}')


def copy_to_outbox(payload, action_type, session):
    payload['actionType'] = action_type
    # remove _id so mongo can create new one in session
    payload.pop('_id', None)
    db[outbox_collection].insert_one(payload, session=session)
    print('outbox: \n', action_type)

####
# Main start function
####
def main():
    events = [insert, update, delete]
    # perform N number of actions
    for idx in range(NUM_RECORDS_TO_GENERATE):
        # for each action, run thru all 3 events:
        for event in events:
            # each event is inside a session
            time.sleep(1)
            with client.start_session(causal_consistency=True) as session:
                event(session=session)


if __name__ == '__main__':
    main()
