# -*- coding:UTF-8 -*-

# author:郭健
# contact: guojian_emails@163.com
# datetime:2021/10/25 14:19
# software: PyCharm

"""
文件说明：
    该模块目前主要实现对消息的持久化
"""
import json
import time
import sqlite3

import MecInnerCommunication

global conn
global module_communicate_client


def init():
    global conn, module_communicate_client
    print("INFO(MessageManager MODULE): INIT BEGIN")
    # 连接用于持久化消息的数据库
    conn = sqlite3.connect("./database_file/message_manager.db", check_same_thread=False)
    # 注册内部通信模块
    module_communicate_client = MecInnerCommunication.module_register("MessageManager", message_hook, on_connect)
    module_communicate_client.subscribe("MessageManager/#", qos=0)
    time.sleep(1)
    print("INFO(MessageManager MODULE): INIT SUCCESS")


# 收到内部通信消息的回调函数
def message_hook(client, userdata, msg):
    print("Module MessageManager Message Received")
    if str(msg.topic).split('/')[-1] == "service":
        service_message_handler(msg.payload)
    else:
        print("ERROR(MessageManager MODULE): Unknown topic type, drop message" + str(msg.topic))


def on_connect(client, userdata, flags, rc):
    message = MecInnerCommunication.generate_mec_json('status', {"module": "MessageManager", "status": "on"}, "MessageManager")
    module_communicate_client.publish("MecInnerCommunication/service", str(message))
    print("Info(MessageManager MODULE REGISTER): Connect Success")


def create_table(device_id):
    cur = conn.cursor()
    sql_text = '''CREATE TABLE IF NOT EXISTS "''' + device_id + '''" (time INTEGER, key_name TEXT, value TEXT);'''
    cur.execute(sql_text)
    conn.commit()
    cur.close


def insert_value(time, device_id, key_name, value):
    cur = conn.cursor()
    sql_text = '''INSERT INTO "''' + device_id + '''" (time, key_name, value) VALUES (''' + str(
        time) + ''', "''' + key_name + '''", "''' + str(value) + '''");'''
    cur.execute(sql_text)
    conn.commit()
    cur.close


def save_into_database(time_stamp, message):
    create_table(message["device_id"])
    insert_value(time_stamp, message["device_id"], message["key"], message["value"])


def service_message_handler(message):
    dic_message = eval(str(message, 'utf-8'))
    if dic_message["Type"] == "calculate.req":
        parse_result = dic_message["Data"]
        save_into_database(dic_message["TimeStamp"], parse_result)
        if MecInnerCommunication.module_is_exit("ModuleManager"):
            module_communicate_client.publish("ModuleManager/service", str(dic_message))
        else:
            print("Error: The ModuleManager module is not registered on the message bus, drop the message")

        if MecInnerCommunication.module_is_exit("DeviceManager"):
            module_communicate_client.publish("DeviceManager/service", str(dic_message))
        else:
            print("Error: The DeviceManager module is not registered on the message bus, drop the message")
    else:
        print("ERROR(MessageManager MODULE): Unknown message type, drop message")


if __name__ == '__main__':
    init()
    save_into_database(125551561156, {"device_id": "device", "key": "key", "value": "12.22"})
    while True:
        time.sleep(10)
