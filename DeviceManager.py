# -*- coding:UTF-8 -*-

# author:郭健
# contact: guojian_emails@163.com
# datetime:2021/11/3 10:27
# software: PyCharm

"""
文件说明：
    实现设备管理
"""
import sqlite3
import time

import MecInnerCommunication

global conn
global module_communicate_client
global device


def init():
    print("INFO(DeviceManager MODULE): INIT BEGIN")
    global conn, module_communicate_client, device
    # connect to the sqlite database
    conn = sqlite3.connect('./database_file/message_manager.db', check_same_thread=False)
    cur = conn.cursor()

    device = {}
    sql_text = '''SELECT name FROM sqlite_master;'''
    cur.execute(sql_text)
    device_list = cur.fetchall()

    # recover data from database
    for i in range(len(device_list)):
        device[device_list[i][0]] = {}
        sql_text = '''SELECT key_name FROM "''' + device_list[i][0] + '''"'''
        cur.execute(sql_text)
        keys = list(set(cur.fetchall()))
        for j in range(len(keys)):
            sql_text = '''SELECT time, value FROM "''' + device_list[i][0] + '''" where key_name = "''' + str(
                keys[j][0]) + '''" order by time desc;'''
            cur.execute(sql_text)
            values = cur.fetchall()
            device[device_list[i][0]][keys[j][0]] = values[0]
    print(device)
    cur.close()

    module_communicate_client = MecInnerCommunication.module_register("DeviceManager", message_hook, on_connect)
    module_communicate_client.subscribe("DeviceManager/#", qos=0)
    time.sleep(1)
    print("INFO(DeviceManager MODULE): INIT SUCCESS")


# 收到内部通信消息的回调函数
def message_hook(client, userdata, msg):
    print("Module DeviceManager Message Received\n")
    if str(msg.topic).split('/')[-1] == "service":
        service_message_handler(msg.payload)
    else:
        print("ERROR(DeviceManager MODULE): Unknown topic type, drop message")


def on_connect(client, userdata, flags, rc):
    message = MecInnerCommunication.generate_mec_json('status', {"module": "DeviceManager", "status": "on"}, "DeviceManager")
    module_communicate_client.publish("MecInnerCommunication/service", str(message))
    print("Info(MessageManager MODULE REGISTER): Connect Success")


def service_message_handler(message):
    dic_message = eval(str(message, 'utf-8'))
    if dic_message["Type"] == "calculate.req":
        parse_result = dic_message["Data"]
        device[parse_result["device_id"]][parse_result["key"]] = (dic_message["TimeStamp"], parse_result["value"])
        print(device)
    else:
        print("ERROR(DeviceManager MODULE): Unknown message type, drop message:" + dic_message["Type"])


if __name__ == '__main__':
    init()
