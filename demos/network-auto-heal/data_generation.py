#!/usr/bin/python
import csv
import datetime
import logging
import os.path
import random
import time
import requests

KV_NUCLIO_FUNCTION_URL = 'http://10.80.1.55:32736'
TSDB_NUCLIO_FUNCTION_URL = 'http://10.80.1.55:30031'
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
    str_rec = ','.join(str(e) for e in rec)
    try:
        response = requests.request("POST", KV_NUCLIO_FUNCTION_URL, data=str_rec, headers={'Content-Type': 'text/plain'})
        if response.status_code != 200:
            logger.error("nuclio function call failed with status code %s for record %s", response.status_code,
                         response.request.body)
        else:
            create_tsdb_record(rec)
    except Exception as e:
        logger.error(str(e))
    pass


# create payload with multiple metrices for ingestion via TSDB nuclio function
def create_tsdb_record(rec):
    metric_util_per = "utilization"
    metric_pkt_rate = "packet_rate"
    metric_latency = "latency"

    sample_time = str(rec[18]*1000)
    latency_value = str(rec[14])
    util_value = str(rec[11])
    pkt_rate_value = str(rec[13])

    label_pe_name = "source_router"
    label_dest_pe_name = "destination_router"
    label_link_status = "link_status"
    label_link_id = "link_id"
    label_direction = "direction_of_traffic"
    label_max_bw = "maximum_bw"

    label_pe_name_value = str(rec[5])
    label_dest_pe_name_value = str(rec[19])
    label_link_status_value = str(rec[12])
    label_link_id_value = str(rec[1])
    label_direction_value = str(rec[2])
    label_max_bw_value = str(rec[8])

    tsdb_payload = "[\n\t{\n\t\t\t\"metric\": \""+metric_util_per+"\",\n\t\t\t\"labels\": {\n\t\t\t\t\""+label_pe_name+"\": \""+label_pe_name_value+"\",\n\t\t\t\t\""+label_dest_pe_name+"\": \""+label_dest_pe_name_value+"\",\n\t\t\t\t\""+label_link_status+"\": \""+label_link_status_value+"\",\n\t\t\t\t\""+label_link_id+"\": \""+label_link_id_value+"\",\n\t\t\t\t\""+label_direction+"\": \""+label_direction_value+"\",\n\t\t\t\t\""+label_max_bw+"\": \""+label_max_bw_value+"\"\n\t\t\t},\n\t\t\t\"samples\": [\n\t\t\t\t{\n\t\t\t\t\t\"t\": \""+sample_time+"\",\n\t\t\t\t\t\"v\": {\n\t\t\t\t\t\t\"N\": "+util_value+"\n\t\t\t\t\t}\n\t\t\t\t}\n\t\t\t]\n\t},\n\t{\n\t\t\t\"metric\": \""+metric_pkt_rate+"\",\n\t\t\t\"labels\": {\n\t\t\t\t\""+label_pe_name+"\": \""+label_pe_name_value+"\",\n\t\t\t\t\""+label_dest_pe_name+"\": \""+label_dest_pe_name_value+"\",\n\t\t\t\t\""+label_link_status+"\": \""+label_link_status_value+"\",\n\t\t\t\t\""+label_link_id+"\": \""+label_link_id_value+"\",\n\t\t\t\t\""+label_direction+"\": \""+label_direction_value+"\",\n\t\t\t\t\""+label_max_bw+"\": \""+label_max_bw_value+"\"\n\t\t\t},\n\t\t\t\"samples\": [\n\t\t\t\t{\n\t\t\t\t\t\"t\": \""+sample_time+"\",\n\t\t\t\t\t\"v\": {\n\t\t\t\t\t\t\"N\": "+pkt_rate_value+"\n\t\t\t\t\t}\n\t\t\t\t}\n\t\t\t]\n\t},\n\t{\n\t\t\t\"metric\": \""+metric_latency+"\",\n\t\t\t\"labels\": {\n\t\t\t\t\""+label_pe_name+"\": \""+label_pe_name_value+"\",\n\t\t\t\t\""+label_dest_pe_name+"\": \""+label_dest_pe_name_value+"\",\n\t\t\t\t\""+label_link_status+"\": \""+label_link_status_value+"\",\n\t\t\t\t\""+label_link_id+"\": \""+label_link_id_value+"\",\n\t\t\t\t\""+label_direction+"\": \""+label_direction_value+"\",\n\t\t\t\t\""+label_max_bw+"\": \""+label_max_bw_value+"\"\n\t\t\t},\n\t\t\t\"samples\": [\n\t\t\t\t{\n\t\t\t\t\t\"t\": \""+sample_time+"\",\n\t\t\t\t\t\"v\": {\n\t\t\t\t\t\t\"N\": "+latency_value+"\n\t\t\t\t\t}\n\t\t\t\t}\n\t\t\t]\n\t}\n]"
    print(tsdb_payload)
    try:
        response = requests.request("POST", TSDB_NUCLIO_FUNCTION_URL, data=tsdb_payload, headers={'Content-Type': 'application/json'})
        if response.status_code != 200:
            logger.error("TSDB ingestion failed with status code %s for record %s", response.status_code,
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
    ingest_record(rec)


def main():
    init_logger()
    if os.path.exists("auto_heal_sample.csv"):
        while True:
            with open('auto_heal_sample.csv', 'r') as dataFile:
                file_reader = csv.reader(dataFile, delimiter=',')
                for row in file_reader:
                    create_records(row)
            dataFile.close()

            # sleeping for one second
            time.sleep(1)
    else:
        logger.error("sample data file not present.existing program ...")


if __name__ == '__main__':
    main()
