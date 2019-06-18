#!/usr/bin/python
import collections
import datetime
import json
import math
import numbers
import os.path

import requests

# Iguazio platform variables
NGINX_HOST = os.getenv('NGINX_HOST')
NGINX_PORT = os.getenv('NGINX_PORT')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')
IGZ_USER = os.getenv('IGZ_USER')
IGZ_PASS = os.getenv('IGZ_PASS')

# Application variables
SCORE_KV = os.getenv('SCORE_KV')
RAW_KV = os.getenv('RAW_KV')
BATCH_SIZE = os.getenv('BATCH_SIZE')
LINK_UP_WAIT_INTERVAL = os.getenv('LINK_UP_WAIT_INTERVAL')
LATENCY_UP_WAIT_INTERVAL = os.getenv('LATENCY_UP_WAIT_INTERVAL')
LATENCY_DOWN_WAIT_INTERVAL = os.getenv('LATENCY_DOWN_WAIT_INTERVAL')
PACKET_RATE_DOWN_WAIT_INTERVAL = os.getenv('PACKET_RATE_DOWN_WAIT_INTERVAL')
PACKET_RATE_UP_WAIT_INTERVAL = os.getenv('PACKET_RATE_UP_WAIT_INTERVAL')
UTILIZATION_50_80_WAIT_INTERVAL = os.getenv('UTILIZATION_50_80_WAIT_INTERVAL')
UTILIZATION_80_100_WAIT_INTERVAL = os.getenv('UTILIZATION_80_100_WAIT_INTERVAL')
UTILIZATION_DOWN_WAIT_INTERVAL = os.getenv('UTILIZATION_DOWN_WAIT_INTERVAL')

logging_context = None


def handler(context, event):
    global logging_context
    logging_context=context
    print('event body log',event.body.decode('utf-8'))
    create_records(event.body.decode('utf-8').split(","), context)


def get_kv_url(kv_name):
    return 'http://{0}:{1}/{2}/{3}/'.format(NGINX_HOST, NGINX_PORT, CONTAINER_NAME, kv_name)


def get_function_headers(v3io_function):
    return {'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            'X-v3io-function': v3io_function,
            'Connection': 'keep-alive'
            }


def validate_string(val):
    if val == 'null':
        return None
    return val


def validate_numeric(val):
    if not val:
        return None
    if not isinstance(val, numbers.Number):
        return None
    return val


def handle_response(response, **kwargs):
    try:
        if response.status_code != 200:
            logging_context.logger.error("ingestion failed with status code %s for record %s", response.status_code,
                                         response.request.body)
    except Exception as e:
        logging_context.logger.error(str(e))


def handle_exception(request, exception):
    logging_context.logger.error(str(exception))


def send_records_raw(network_data_score):
    try:

        jmsg_raw = json.dumps(get_structured_message_raw(network_data_score), indent=4)
        year = network_data_score[21]
        month = network_data_score[22]
        day = network_data_score[23]
        partition = "year=" + str(year) + "/month=" + str(month) + "/day=" + str(day) + "/"
        url = get_kv_url(RAW_KV) + partition
        payload = jmsg_raw
        headers = get_function_headers('PutItem')
        # url = get_kv_url(RAW_KV)
        response = requests.post(url, auth=(IGZ_USER, IGZ_PASS), data=payload, headers=headers)
        if response.status_code != 200:
            logging_context.logger.error("historical record ingestion failed with status code %s for record %s", response.status_code,
                                         response.request.body)
    except Exception as e:
        logging_context.logger.error(str(e))
        pass


def send_records_score(record):
    try:
        payload = record
        headers = get_function_headers('PutItem')
        url = get_kv_url(SCORE_KV)
        response = requests.post(url, auth=(IGZ_USER, IGZ_PASS), data=payload, headers=headers)
        if response.status_code != 200:
            logging_context.logger.error("score ingestion failed with status code %s for record %s", response.status_code,
                                         response.request.body)
    except Exception as e:
        logging_context.logger.error(str(e))
        pass


def get_structured_message_raw(msg):
    json_dict = collections.OrderedDict([
        ('Key', collections.OrderedDict([('id', {'S': validate_string(msg[0])})])),
        ('Item', collections.OrderedDict([
            ('link_id', {'S': validate_string(msg[1])}),
            ('direction', {'S': validate_string(msg[2])}),
            ('qos_id', {'N': (msg[3])}),
            ('pe_add', {'S': validate_string(msg[4])}),
            ('pe_name', {'S': validate_string(msg[5])}),
            ('dest_pe_name', {'S': validate_string(msg[19])}),
            ('src_interface', {'S': validate_string(msg[6])}),
            ('dest_interface', {'S': validate_string(msg[20])}),
            ('util', {'N': str(msg[7])}),
            ('max_bw', {'N': str(msg[8])}),
            ('curr_cost1', {'N': str(msg[9])}),
            ('curr_cost2', {'N': str(msg[10])}),
            ('util_per', {'N': str(msg[11])}),
            ('link_status', {'S': validate_string(msg[12])}),
            ('pkt_rate', {'N': str(msg[13])}),
            ('latency', {'N': str(msg[14])}),
            ('avg_latency', {'N': str(msg[15])}),
            ('base_avg_latency', {'N': str(msg[16])}),
            ('alarm', {'N': str(msg[17])}),
            ('time_stamp', {'N': str(msg[18])}),
            ('year', {'N': str(msg[21])}),
            ('month', {'N': str(msg[22])}),
            ('day', {'N': str(msg[23])}),
            ('total_score', {'N': str(msg[24])})
        ]))
    ])
    return json_dict


def get_structured_message_score_exists(rec):
    item_dict = get_items_ordered_collection(rec)
    json_dict = collections.OrderedDict([
        ('Key', collections.OrderedDict([('id', {'S': validate_string(rec['Item']['id']['S'])})])),
        ('Item', item_dict)
    ])
    return json_dict


def get_structured_message_score_new(network_data):
    item_dict = get_empty_score_items(network_data)  # linkId, pe_name,interface,base latency
    json_dict = collections.OrderedDict([
        ('Key', collections.OrderedDict([('id', {'S': validate_string(network_data[1] + "." + network_data[3])})])),
        # key as linkId.QOSID
        ('Item', item_dict)
    ])
    return json_dict


def get_items_ordered_collection(msg):
    coll = collections.OrderedDict()

    coll['link_id'] = {'S': msg['Item']['link_id']['S']}
    coll['pe_name'] = {'S': msg['Item']['pe_name']['S']}
    coll['pe_name_dest'] = {'S': msg['Item']['pe_name_dest']['S']}
    coll['interface'] = {'S': msg['Item']['interface']['S']}
    coll['dest_interface'] = {'S': msg['Item']['dest_interface']['S']}
    coll['curr_cost1'] = {'N': str(msg['Item']['curr_cost1']['N'])}
    coll['curr_cost2'] = {'N': str(msg['Item']['curr_cost2']['N'])}
    coll['max_bw'] = {'N': str(msg['Item']['max_bw']['N'])}
    coll['peak_util'] = {'N': str(float((msg['Item']['peak_util']['N'])))}
    coll['pkt_rate'] = {'N': str(msg['Item']['pkt_rate']['N'])}
    coll['avg_latency'] = {'N': str(msg['Item']['avg_latency']['N'])}
    coll['base_avg_latency'] = {'N': str(msg['Item']['base_avg_latency']['N'])}
    coll['latency_up_count'] = {'N': str(msg['Item']['latency_up_count']['N'])}
    coll['latency_down_count'] = {'N': str(msg['Item']['latency_down_count']['N'])}
    coll['link_up_count'] = {'N': str(msg['Item']['link_up_count']['N'])}
    coll['util_down_count'] = {'N': str(msg['Item']['util_down_count']['N'])}
    coll['util_50_80_count'] = {'N': str(msg['Item']['util_50_80_count']['N'])}
    coll['util_80_100_count'] = {'N': str(msg['Item']['util_80_100_count']['N'])}
    coll['packet_rate_up_count'] = {'N': str(msg['Item']['packet_rate_up_count']['N'])}
    coll['packet_rate_down_count'] = {'N': str(msg['Item']['packet_rate_down_count']['N'])}
    coll['latency_score'] = {'N': str(msg['Item']['latency_score']['N'])}
    coll['util_score'] = {'N': str(msg['Item']['util_score']['N'])}
    coll['packet_rate_score'] = {'N': str(msg['Item']['packet_rate_score']['N'])}
    coll['link_status_score'] = {'N': str(msg['Item']['link_status_score']['N'])}
    coll['timestamp'] = {'N': str(msg['Item']['timestamp']['N'])}
    coll['total_score'] = {'N': str(msg['Item']['total_score']['N'])}

    return coll


def get_empty_score_items(network_data):
    coll = collections.OrderedDict()

    coll['link_id'] = {'S': network_data[1]}
    coll['pe_name'] = {'S': network_data[5]}
    coll['pe_name_dest'] = {'S': network_data[19]}
    coll['interface'] = {'S': network_data[6]}
    coll['dest_interface'] = {'S': network_data[20]}
    coll['base_avg_latency'] = {'N': network_data[16]}
    coll['avg_latency'] = {'N': network_data[15]}
    coll['curr_cost1'] = {'N': str(network_data[9])}
    coll['curr_cost2'] = {'N': str(network_data[10])}
    coll['max_bw'] = {'N': str(network_data[8])}
    coll['peak_util'] = {'N': str(network_data[7])}  # for initialisation of new row , current util is set to peak util.
    coll['pkt_rate'] = {'N': str(network_data[13])}
    coll['avg_latency'] = {'N': str(network_data[15])}
    coll['base_avg_latency'] = {'N': str(network_data[16])}
    coll['latency_up_count'] = {'N': '0'}
    coll['latency_down_count'] = {'N': '0'}
    coll['link_up_count'] = {'N': '0'}
    coll['util_down_count'] = {'N': '0'}
    coll['util_50_80_count'] = {'N': '0'}
    coll['util_80_100_count'] = {'N': '0'}
    coll['packet_rate_up_count'] = {'N': '0'}
    coll['packet_rate_down_count'] = {'N': '0'}
    coll['latency_score'] = {'N': '0'}
    coll['util_score'] = {'N': '0'}
    coll['packet_rate_score'] = {'N': '0'}
    coll['link_status_score'] = {'N': '0'}
    coll['timestamp'] = {'N': str(network_data[18])}
    coll['total_score'] = {'N': '0'}

    return coll


def get_score_record(key):
    try:
        python_dictionary = {'AttributesToGet': '*'}
        score = json.dumps(python_dictionary, indent=4)
        response = requests.post(get_kv_url(SCORE_KV) + key,
                                 auth=(IGZ_USER, IGZ_PASS), data=score, headers=get_function_headers('GetItem'))
        if response.status_code != 404:
            score = json.loads(response.content)
        # score = yaml.safe_load(response.content)
        else:
            score = []
        return score
    except Exception as e:
        logging_context.logger.error(str(e))
        pass


def calculate_score_link_status(status, record):
    if status == 'DOWN':
        record['Item']['link_up_count']['N'] = 0
        record['Item']['link_status_score']['N'] = 15
    elif status == 'UP' and int(record['Item']['link_up_count']['N']) < int(LINK_UP_WAIT_INTERVAL):
        record['Item']['link_up_count']['N'] = int(record['Item']['link_up_count']['N']) + 1
    else:
        record['Item']['link_up_count']['N'] = 0
        record['Item']['link_status_score']['N'] = 0
    return record


def calculate_score_latency(latency, record):
    if latency < 0 and latency != -1:
        if int(record['Item']['latency_up_count']['N']) < int(LATENCY_UP_WAIT_INTERVAL) and int(
                record['Item']['latency_score']['N']) != 8:
            record['Item']['latency_up_count']['N'] = int(record['Item']['latency_up_count']['N']) + 1
            record['Item']['latency_down_count']['N'] = 0
        else:
            record['Item']['latency_score']['N'] = 8
            record['Item']['latency_down_count']['N'] = 0
    elif latency == -1:  # retain the previous counters if latency = -1
        pass
    else:
        if int(record['Item']['latency_down_count']['N']) < int(LATENCY_DOWN_WAIT_INTERVAL):
            record['Item']['latency_down_count']['N'] = int(record['Item']['latency_down_count']['N']) + 1
            record['Item']['latency_up_count']['N'] = 0
        else:
            record['Item']['latency_score']['N'] = 0
            record['Item']['latency_up_count']['N'] = 0
            record['Item']['latency_down_count']['N'] = 0
    return record


def calculate_score_pkt_drop(pkt_rate, record):
    if pkt_rate < 100:  # pkt_rate =100 means no packet drop
        if int(record['Item']['packet_rate_down_count']['N']) < int(PACKET_RATE_DOWN_WAIT_INTERVAL) and int(
                record['Item']['packet_rate_score']['N']) != 8:
            record['Item']['packet_rate_up_count']['N'] = 0
            record['Item']['packet_rate_down_count']['N'] = int(record['Item']['packet_rate_down_count']['N']) + 1
            record['Item']['packet_rate_score']['N'] = 5
        else:
            record['Item']['packet_rate_score']['N'] = 8
            record['Item']['packet_rate_up_count']['N'] = 0
            record['Item']['packet_rate_down_count']['N'] = int(record['Item']['packet_rate_down_count']['N']) + 1
    else:
        if int(record['Item']['packet_rate_up_count']['N']) < int(PACKET_RATE_UP_WAIT_INTERVAL):
            record['Item']['packet_rate_down_count']['N'] = 0
            if int(record['Item']['packet_rate_up_count']['N']) <= 20:  # increment the packet rate up count to max 20
                record['Item']['packet_rate_up_count']['N'] = int(record['Item']['packet_rate_up_count']['N']) + 1
        else:
            record['Item']['packet_rate_score']['N'] = 0
            if int(record['Item']['packet_rate_up_count']['N']) <= 20:  # increment the packet rate up count to max 20
                record['Item']['packet_rate_up_count']['N'] = int(record['Item']['packet_rate_up_count']['N']) + 1
            record['Item']['packet_rate_down_count']['N'] = 0
    return record


def calculate_score_util(util_per, record):
    if not math.isnan(
            util_per):  # check if utilization percentage is Nan , else retain the previous values of score record.
        if util_per >= 80:
            if int(record['Item']['util_80_100_count']['N']) >= int(UTILIZATION_80_100_WAIT_INTERVAL) and int(
                    record['Item']['util_score']['N']) == 5:
                record['Item']['util_80_100_count']['N'] = int(record['Item']['util_80_100_count']['N']) + 1
                record['Item']['util_50_80_count']['N'] = 0
                record['Item']['util_down_count']['N'] = 0
                record['Item']['util_score']['N'] = 8
            else:
                record['Item']['util_80_100_count']['N'] = int(record['Item']['util_80_100_count']['N']) + 1
                record['Item']['util_50_80_count']['N'] = 0
                record['Item']['util_down_count']['N'] = 0
        elif 80 > util_per > 50:
            if int(record['Item']['util_50_80_count']['N']) >= int(UTILIZATION_50_80_WAIT_INTERVAL):
                record['Item']['util_50_80_count']['N'] = int(record['Item']['util_50_80_count']['N']) + 1
                record['Item']['util_80_100_count']['N'] = 0
                record['Item']['util_down_count']['N'] = 0
                record['Item']['util_score']['N'] = 5
            else:
                record['Item']['util_50_80_count']['N'] = int(record['Item']['util_50_80_count']['N']) + 1
                record['Item']['util_80_100_count']['N'] = 0
                record['Item']['util_down_count']['N'] = 0
        else:
            if util_per <= 50 and int(record['Item']['util_down_count']['N']) <= int(UTILIZATION_DOWN_WAIT_INTERVAL):
                if int(record['Item']['util_down_count']['N']) <= 20:  # increment the utilizaton down count to max 20
                    record['Item']['util_down_count']['N'] = int(record['Item']['util_down_count']['N']) + 1
                record['Item']['util_80_100_count']['N'] = 0
                record['Item']['util_50_80_count']['N'] = 0
            else:
                record['Item']['util_score']['N'] = 0
                if int(record['Item']['util_down_count']['N']) <= 20:  # increment the utilizaton down count to max 20
                    record['Item']['util_down_count']['N'] = int(record['Item']['util_down_count']['N']) + 1
                record['Item']['util_80_100_count']['N'] = 0
                record['Item']['util_50_80_count']['N'] = 0
    return record


def calculate_peak_util(util, record, ts_day_new):
    d_existing = datetime.datetime.utcfromtimestamp(int(record['Item']['timestamp']['N'])) + datetime.timedelta(
        hours=+8)  # convert utc time to singapore time
    ts_day_existing = d_existing.day

    if ts_day_new == ts_day_existing:
        if util > float(record['Item']['peak_util']['N']):
            record['Item']['peak_util']['N'] = util
    else:
        record['Item']['peak_util']['N'] = util
    return record


def create_records(rec, context):
    network_data = tuple(rec)
    score_record = get_score_record(network_data[1] + "." + network_data[3])
    if score_record:
        score_record = calculate_score_link_status(network_data[12], score_record)
        score_record = calculate_score_pkt_drop(int(network_data[13]), score_record)
        score_record = calculate_score_latency(float(network_data[14]), score_record)
        score_record = calculate_score_util(float(network_data[11]), score_record)  # type: int
        score_record = calculate_peak_util(float(network_data[7]), score_record, network_data[23])
        score_record['Item']['total_score']['N'] = int(score_record['Item']['latency_score']['N']) + int(
            score_record['Item']['util_score']['N']) + int(score_record['Item']['packet_rate_score']['N']) + int(
            score_record['Item']['link_status_score']['N'])
        score_record['Item']['timestamp']['N'] = int(network_data[18])
        jmsg_score = json.dumps(
            get_structured_message_score_exists(score_record))  # False is set for record which already exists.
        network_data_score = network_data + (score_record['Item']['total_score']['N'],)
    else:
        jmsg_score = json.dumps(get_structured_message_score_new(network_data))
        network_data_score = network_data + (0,)
    send_records_raw(network_data_score)
    send_records_score(jmsg_score)

##for test
# rec = ['1052.1_50', '1052', '1', '16777322', '202.163.59.82', 'SNGPC-ER3', 'ge-5/3/0.0', '23462079.81', 125000000L, 'UP', 100, '1.0000', 0, 1524131434]
# create_records(rec)
