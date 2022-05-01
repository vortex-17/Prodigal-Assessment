import time
import urllib.request
from datetime import datetime, timedelta
from pymongo import MongoClient,InsertOne
import multiprocessing
import pika
import json
import logging
import argparse
import schedule

logging.basicConfig(filename='error.log', level=logging.DEBUG)

def date_feeder():
    date_list = []
    start_date = datetime.fromisoformat('2022-03-01')
    today_date = datetime.now() - timedelta(days = 1)
    # temp_date = start_date
    while start_date < today_date:
        next_date = start_date + timedelta(days = 10)
        if next_date > today_date:
            date_list.append([start_date.strftime('%d-%b-%Y'), today_date.strftime('%d-%b-%Y')])
            return date_list
        
        date_list.append([start_date.strftime('%d-%b-%Y'), next_date.strftime('%d-%b-%Y')])
        start_date = next_date + timedelta(days = 1)

    return date_list


def normalise_scheme_name(data):
    data.replace("multi cap", "multicap")
    options = ["growth", "idcw", "income distribution cum capital withdrawal"]
    plans = ["direct", "regular"]
    option = ""
    plan = ""

    for i in options:
        if i in data:
            option = i
            break
    
    for i in plans:
        if i in data:
            plan = i
            break

    return data, option, plan


def crawler(date_list):
    t1 = time.time()
    url = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={}&todt={}".format(date_list[0], date_list[1])
    if url:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        channel.queue_declare(queue='mongo_insert')
        try:
            data = urllib.request.urlopen(url)
            print(data)
            count = 0
            tag = ""
            requests = [{"start_time": t1, "start_date" : date_list[0], "end_date": date_list[1]}]
            for data_line in data:
                #Removing the first line
                if count == 0:
                    count+=1
                    continue
                
                data_line = data_line.decode()
                data_line = data_line.rstrip()
                # print(data_line)
                data_line = data_line.split(";")
                if len(data_line) < 8:
                    #It is a tag or name of the company
                    if "Schemes" in data_line[0]:
                        tag = data_line[0]
                    continue

                data_line.append(tag)
                # print(data_line)

                d, option, plan = normalise_scheme_name(data_line[1].lower())

                # entry_dict = {"scheme_code": data_line[0], "scheme_name": data_line[1].lower(), "isin_div_payout": data_line[2].lower(), "isin_div_reinvestment": data_line[3].lower(), \
                #  "net_value_asset": data_line[4], "repurchase_price": data_line[5], "sale_price": data_line[6], "date": data_line[7].lower(), "type": data_line[8].lower()}

                entry_dict = {"scheme_code": data_line[0], "scheme_name": d,"option" : option, "plan" : plan, "isin_div_payout": data_line[2].lower(), "isin_div_reinvestment": data_line[3].lower(), \
                 "net_value_asset": data_line[4], "repurchase_price": data_line[5], "sale_price": data_line[6], "date": data_line[7].lower(), "type": data_line[8].lower()}
                # print(data_line)

                requests.append(entry_dict)


            if len(requests) < 3:
                print("No data found for this date")
                return time.time()-t1

            channel.basic_publish(exchange='', routing_key='mongo_insertion', body=json.dumps(requests))
            
            # print(requests)
          
        except Exception as e:
            print("error occurred: ", e)
            logging.error(datetime.now().strftime('%d-%b-%Y') + "," + date_list[0] + "," + date_list[1] + "," + e)
            return time.time()-t1
        
        return time.time()-t1
     
def inital_fetch():
    pool = multiprocessing.Pool()
    t1 = time.time()
    date_list = date_feeder()
    outputs = pool.map(crawler, date_list)
    print("Output: {}".format(outputs))
    print("Time Taken : ",time.time()-t1)
    pool.close()

def daily_fetch():
    t1 = time.time()
    date_list = [datetime.now().strftime('%d-%b-%Y'), datetime.now().strftime('%d-%b-%Y')]
    output = crawler(date_list)
    print(output)
    print("Time Taken : ",time.time()-t1)

   
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--initial-fetch", default=False, action="store")
    arguments = parser.parse_args()
    print(arguments)
    if getattr(arguments, "initial_fetch"):
        inital_fetch()

    # schedule.every().day.at("01:00").do(daily_fetch,'It is 01:00')

    # while True:
    #     schedule.run_pending()

    daily_fetch()
