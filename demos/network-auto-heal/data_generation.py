#!/usr/bin/python
import csv
import datetime
import logging
import os.path
import random
import time
import requests

NUCLIO_FUNCTION_URL = 'http://10.80.1.55:32736'
logger = None


def init_logger():
    global logger
    if logger is None:
        logger = logging.getLogger('data generation and ingestion via nuclio')
    handler = logging.FileHandler('logs/ingestion.log')
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)


def handle_response(response, **kwargs):
    try:
        if response.status_code != 200:
            logger.error("ingestion failed with status code %s for record %s", response.status_code,
                         response.request.body)
    except Exception as e:
        logger.error(str(e))


def handle_exception(request, exception):
    logger.error(str(exception))


# ingest record by calling the nuclio function
def ingest_record(rec):
    try:
        response = requests.request("POST", NUCLIO_FUNCTION_URL, data=rec, headers={'Content-Type': 'text/plain'})
        print(response.request.data)
        if response.status_code != 200:
            logger.error("nuclio function call failed with status code %s for record %s", response.status_code,
                         response.request.body)
    except Exception as e:
        logger.error(str(e))
    pass


def create_records(rec):
    # utilization
    rec[7] = random.uniform(float(rec[8]) * .1, float(rec[8]) * .7)

    # util percentage
    rec[11] = (rec[7] / float(rec[8])) * 100

    # packet rate
    rec[13] = random.randint(95, 100)

    # latency
    rec[14] = random.randint(1, 50)

    # timestamp
    rec[18] = int(time.time())
    d = datetime.datetime.utcfromtimestamp(rec[18]) + datetime.timedelta(
        hours=+8)  # convert utc time to singapore time
    rec[21] = d.year
    rec[22] = d.month
    rec[23] = d.day

    # historical record key
    rec[0] = rec[0].split("_")[0]+"_"+str(rec[18])
    str_rec = ','.join(str(e) for e in rec)
    ingest_record(str_rec)
    #print(str_rec)


def main():
    init_logger()
    if os.path.exists("auto_heal_sample.csv"):
        while True:
            with open('auto_heal_sample.csv', 'r') as dataFile:
                file_reader = csv.reader(dataFile, delimiter=',')
                for row in file_reader:
                    create_records(row)
            dataFile.close()
    else:
        logger.error("sample data file not present.existing program ...")


if __name__ == '__main__':
    main()
