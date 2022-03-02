# coding=utf8
import importlib
import json
import sys
import time

reload(sys)
sys.path.append('.')
sys.setdefaultencoding('utf-8')
sys.path.append('.')


def pcr_cpp_schema(line):
    return line["after"]


def pcr_cpp_filter(line):
    return "TEST_CODE" in line and line["TEST_CODE"] in ["NCOV", "NCOV2"]


def pcr_cpp_table(line):
    norm_dict = {"event": "art", "event_time": line["check_time"]}

    return norm_dict


def art_cpp_schema(line):
    return line["data"]


def art_cpp_filter(line):
    return "result" in line and line["result"] in ["Positive", "Negative"]


def art_cpp_table(line):
    norm_dict = {"event": "art", "event_time": line["check_time"]}

    return norm_dict


handler_func_dict = {
    "dwd_pcr": {"cpp_schema": "pcr_cpp_schema",
                "cpp_filter": "pcr_cpp_filter",
                "cpp_table": "pcr_cpp_table"}
    , "dwd_art": {"cpp_schema": "art_cpp_schema",
                  "cpp_filter": "art_cpp_filter",
                  "cpp_table": "art_cpp_table"}}
module = "scripts.norm_udf"


def run_cpp_func(topic, op_type, line):
    handler_func = handler_func_dict[topic][op_type]
    model_m = importlib.import_module(module)
    func_run = getattr(model_m, handler_func)
    return func_run(line)


def send_kafka(producer, topic, key, message_body):
    timestamp = int(time.time())
    message = {
        "id": "%s-%s" % (key, timestamp),
        "occur_time": timestamp,
        "priority": "5",
        "from": "data",
        "type": "object",
        "data": message_body
    }
    future = producer.send(topic=topic, key=key, value=json.dumps(message))
    future.get(timeout=10)


def mapping_unique_id(redis_conn, message):
    unique_key = message["key"]
    if unique_key:
        unique_id = redis_conn.hget("DATA:UNIQUE:KEY", unique_key)
        if unique_id:
            message["unique_id"] = unique_id
        else:
            message["unique_id"] = ""

    return message
