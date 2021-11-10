# -*- coding:UTF-8 -*-

# author:郭健
# contact: guojian_emails@163.com
# datetime:2021/10/19 13:26
# software: PyCharm

"""
文件说明：
    实现设备与服务之间的映射，以及映射的持久化
"""
import json
import sqlite3
import time

import MecInnerCommunication

dic_device_module = {}
global conn
global module_communicate_client


def init():
    print("INFO(ModuleManager MODULE): INIT BEGIN")
    global conn, module_communicate_client
    # connect to the sqlite database
    conn = sqlite3.connect('./database_file/module_manager.db', check_same_thread=False)
    cur = conn.cursor()
    # get device list in the databases
    sql_text = '''SELECT name FROM sqlite_master;'''
    cur.execute(sql_text)
    device_list = cur.fetchall()
    print(len(device_list), "devices in the module manager")
    # recover data from database
    for i in range(len(device_list)):
        module_list = {}  # use list to save modules in the ram
        sql_text = '''SELECT key, module FROM "''' + device_list[i][0] + '''"'''
        cur.execute(sql_text)
        result = cur.fetchall()
        for j in range(len(result)):
            module_list[result[j][0]] = result[j][1]
        dic_device_module[device_list[i][0]] = module_list
    print(dic_device_module)
    cur.close()

    # 注册内部通信模块
    module_communicate_client = MecInnerCommunication.module_register("ModuleManager", message_hook, on_connect)
    module_communicate_client.subscribe("ModuleManager/#", qos=0)
    time.sleep(1)
    print("INFO(ModuleManager MODULE): INIT SUCCESS")


def on_connect(client, userdata, flags, rc):
    message = MecInnerCommunication.generate_mec_json('status', {"module": "ModuleManager", "status": "on"}, "ModuleManager")
    module_communicate_client.publish("MecInnerCommunication/service", str(message))
    print("Info(ModuleManager MODULE REGISTER): Connect Success")


# 收到内部通信消息的回调函数
def message_hook(client, userdata, msg):
    print("Module ModuleManager Message Received")
    if str(msg.topic).split('/')[-1] == "service":
        service_message_handler(msg.payload)
    else:
        print("ERROR(ModuleManager MODULE): Unknown topic type, drop message")


def service_message_handler(payload):
    dic_message = eval(str(payload, 'utf-8'))
    if dic_message["Type"] == "calculate.req":
        parse_result = dic_message["Data"]
        module_name = get_module_name(parse_result)
        if module_name is None:
            print("INFO(ModuleManager MODULE):There are no matching modules: " + parse_result['device_id'] + ", " +
                  parse_result['key'] + ", drop message")
        elif MecInnerCommunication.module_is_exit(module_name):
            module_communicate_client.publish(module_name + "/service", str(dic_message))
        else:
            print("Error: The " + module_name + " module is not registered on the message bus, drop the message")
    else:
        print("ERROR(ModuleManager MODULE): Unknown message type, drop message")


def delete_device_module_database(device_id, key):
    cur = conn.cursor();
    sql_text = '''DELETE FROM "''' + device_id + '''" WHERE key="''' + key + '''"'''
    cur.execute(sql_text)
    conn.commit()
    cur.close


def create_device_module_database(device_id):
    cur = conn.cursor()
    sql_text = '''CREATE TABLE IF NOT EXISTS "''' + device_id + '''" (key TEXT, module TEXT);'''
    cur.execute(sql_text)
    conn.commit()
    cur.close


def add_module_to_device_database(device_id, key, module):
    cur = conn.cursor()
    sql_text = '''INSERT INTO "''' + device_id + '''" (key, module) VALUES ("''' + key + '''", "''' + module + '''");'''
    print(sql_text)
    cur.execute(sql_text)
    conn.commit()
    cur.close


def update_module_to_device_database(device_id, key, module):
    cur = conn.cursor()
    sql_text = '''UPDATE "''' + device_id + '''" SET module="''' + module + '''" WHERE key="''' + key + '''";'''
    print(sql_text)
    cur.execute(sql_text)
    conn.commit()
    cur.close


def delete_device_module(device_id, key):
    if dic_device_module.get(device_id) is None:
        print("Error: Can't find the device, please check")
        return None
    if dic_device_module[device_id].get(key) is None:
        print("Error: Can't find the module by device id, please check")
        return None
    dic_device_module[device_id].remove(key)
    delete_device_module_database(device_id, key)


def add_device_module(device_id, key, module_name):
    if dic_device_module.get(device_id) is None:
        create_device_module_database(device_id)
        dic_device_module[device_id] = {}
        dic_device_module[device_id][key] = module_name
        add_module_to_device_database(device_id, key, module_name)
    elif dic_device_module[device_id].get(key) is None:
        dic_device_module[device_id][key] = module_name
        add_module_to_device_database(device_id, key, module_name)
    else:
        dic_device_module[device_id][key] = module_name
        update_module_to_device_database(device_id, key, module_name)
    print("add module " + module_name + " to " + "device_id success")


def get_module_name(message):
    if dic_device_module.get(message["device_id"]) is None:
        print("The device:" + message["device_id"] + ", key:" + message["key"] + " has no calculate module")
        return None
    elif dic_device_module[message["device_id"]].get(message["key"]) is None:
        print("The device:" + message["device_id"] + ", key:" + message["key"] + " has no calculate module")
        return None
    else:
        return dic_device_module[message["device_id"]].get(message["key"])


if __name__ == '__main__':
    init()
    while True:
        print(dic_device_module)
        time.sleep(5)
