# -*- coding:UTF-8 -*-

# author:郭健
# contact: guojian_emails@163.com
# datetime:2021/10/28 10:35
# software: PyCharm

"""
文件说明：
    实现内部模块的注册
"""
import json
import time
import paho.mqtt.client as mqtt

SOUTH_HOST = "47.106.121.88"
PORT = 1884
KEEP_ALIVE = 600

module_list = []
global client_mec


# def on_publish(client, userdata, mid):
#     print("Publish Success")


def on_disconnect(client, userdata, rc):
    print("The mqtt connection is down, try reconnect")
    reconnect_error = client.reconnect()
    while reconnect_error != 0:
        print("Reconnect fail, the error code is" + reconnect_error)
    print("Reconnect Success")


# 构建内部通信的消息
def generate_mec_json(type, data, src_module):
    # 输入是一个字典
    json_message = json.loads(json.dumps({}))
    json_message['TimeStamp'] = int(round(time.time() * 1000))
    json_message['Type'] = type
    json_message['Src'] = src_module
    json_message['Data'] = data
    return json_message


def module_is_exit(module_name):
    return module_list.count(module_name) > 0


def module_register(module_name, message_hook, connect_hook):
    client = mqtt.Client()
    client.on_connect = connect_hook
    client.on_message = message_hook
    # client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.username_pw_set("mosquitto", "mosquitto")
    will_message = generate_mec_json('status', {"module": module_name, "status": "off"}, module_name)
    client.will_set("MecInnerCommunication/service", str(will_message), 0, False)
    client.connect(SOUTH_HOST, PORT, KEEP_ALIVE)
    client.loop_start()
    print(module_name + "Register Communication Success")
    return client


def init():
    print("INFO(MecInnerCommunication MODULE): INIT BEGIN")
    global client_mec
    client_mec = mqtt.Client()
    client_mec.on_connect = on_connect
    client_mec.on_message = message_hook
    client_mec.on_disconnect = on_disconnect
    client_mec.username_pw_set("mosquitto", "mosquitto")
    will_message = generate_mec_json('status', {"module": "MecInnerCommunication", "status": "off"}, "MecInnerCommunication")
    client_mec.will_set("AllModule/service", str(will_message), 0, False)
    client_mec.connect(SOUTH_HOST, PORT, KEEP_ALIVE)
    client_mec.loop_start()
    client_mec.subscribe("MecInnerCommunication/#", 0)
    print("INFO(MecInnerCommunication MODULE): INIT SUCCESS")


def on_connect(client, userdata, flags, rc):
    message = generate_mec_json('status', {"module": "MecInnerCommunication", "status": "on"}, "MecInnerCommunication")
    client_mec.publish("AllModule/service", str(message))
    print("Info(MecInnerCommunication MODULE REGISTER): Connect Success")


# 收到内部通信消息的回调函数
def message_hook(client, userdata, msg):
    print("Module MecInnerCommunication Message Received")
    print(str(msg.payload, "utf-8"))
    if str(msg.topic).split('/')[-1] == "service":
        service_message_handler(client, msg.payload)
    else:
        print("ERROR(MecInnerCommunication MODULE): Unknown topic type, drop message")


def service_message_handler(client, payload):
    dic_message = eval(str(payload, 'utf-8'))
    if dic_message["Type"] == "status":
        parse_result = dic_message["Data"]
        if parse_result['status'] == "off":
            if module_list.count(parse_result['module']) > 0:
                module_list.remove(parse_result['module'])
        elif parse_result['status'] == "on":
            if module_list.count(parse_result['module']) <= 0:
                module_list.append(parse_result['module'])
        else:
            print("ERROR(MecInnerCommunication MODULE): Unknown message, drop message")
    elif dic_message["Type"] == "getModuleList.req":
        parse_result = dic_message["Data"]
        message_reply = generate_mec_json("getModuleList.rep", str(module_list), "MecInnerCommunication")
        client_mec.publish(parse_result["module"] + "/service", str(message_reply))
    else:
        print("ERROR(MecInnerCommunication MODULE): Unknown message type, drop message")