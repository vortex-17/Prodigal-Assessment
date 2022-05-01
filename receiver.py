#!/usr/bin/env python
import pika, sys, os
import json
from pymongo import MongoClient,InsertOne
import pymongo
import time
from datetime import datetime
import logging

logging.basicConfig(filename='mongo_error.log', level=logging.DEBUG)

def get_database(url = None, db_name = ""):
    if url:
        client = MongoClient(url)
        return client[db_name]

    
    return "Please provide a valid url"

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='mongo_insert')
    # mongo_url = 'mongodb+srv://vortex-17:<password>@cluster0.yijap.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
    mongo_url = 'mongodb://127.0.0.1:27017/'
    dbname = get_database(mongo_url, "test_prodigal")
    mf_data_collection = dbname["MUTUAL_FUND_DATA"]
    mf_data_collection.create_index([("scheme_code", pymongo.DESCENDING)])
    # misc_collection = dbname["LOG"]

    def callback(ch, method, properties, body):
        t1 = time.time()
        data = json.loads(body)
        print(" [x] Received:", data[0])
        inserted = False
        try:
            mf_data_collection.insert_many(data)
            inserted = True
        except Exception as e:
            body.reject()
            print("Could not write into database. Error message: ",e)
            logging.error(datetime.now().strftime('%d-%b-%Y') + "," + data[0] + "," + e)

        # if inserted:
        #     try:
        #         latest_data = misc_collection.find()
        #     except Exception as e:
        #         print("Error: ", e)
        print("Time to insert into MongoDB: ", time.time()-t1)

    channel.basic_consume(queue='mongo_insertion', on_message_callback=callback, auto_ack=True)

    # print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)