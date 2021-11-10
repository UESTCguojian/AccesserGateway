# -*- coding:UTF-8 -*-

# author:郭健
# contact: guojian_emails@163.com
# datetime:2021/10/25 10:03
# software: PyCharm

"""
文件说明：
    南向通信系统
    对外提供发送消息接口 send_data(device_id, data)
    {"device_id": device_id, "key_name": key, "value": value}
"""
import json
import time
import paho.mqtt.client as mqtt

import MecInnerCommunication

dic_id_gateway = {}
# 南向MQTT连接参数
client_south = mqtt.Client()
SOUTH_HOST = "47.106.121.88"
PORT = 1883
KEEP_ALIVE = 600
SUBSCRIBE_TOPIC = 'BY2/U/#'  # 初始化南向需要订阅的TOPIC

global module_communicate_client


def generate_south_send_topic(device_id, type):
    try:
        topic = dic_id_gateway[device_id]
    except:
        print("Error")
    return 'BY2/D/' + device_id + '/' + type


def on_publish(client, userdata, mid):
    print("Publish Success\n")


def on_disconnect(client, userdata, rc):
    print("The mqtt connection is down, try reconnect")
    reconnect_error = client.reconnect()
    while reconnect_error != 0:
        print("Reconnect fail, the error code is" + reconnect_error)
    print("Reconnect Success")


def on_message_south(client, userdata, msg):
    print("South mqtt message arrived\n")
    # 将南向消息解析为需要的形式{"device_id": value, "key": value, "value": value}
    parse_result = parse_message(msg.payload)
    # 将解析完的消息生成为服务器内部通信的消息
    message_mec = MecInnerCommunication.generate_mec_json("calculate.req", parse_result, "SouthCommunication")
    print(message_mec)
    # 消息投递
    # 投递给Message先进行持久化
    if MecInnerCommunication.module_is_exit("MessageManager"):
        module_communicate_client.publish("MessageManager/service", str(message_mec))
    else:
        print("Error: The MessageManager module is not registered on the message bus, drop the message")


def on_connect_south(client, userdata, flags, rc):
    print("South Connect Success")
    client.subscribe(SUBSCRIBE_TOPIC, qos=0)




def mqtt_south_send(topic, payload, qos):
    client_south.publish(topic, payload, qos)
    print("Mqtt north publish success")


def parse_message(payload):
    dic_payload = eval(str(payload, 'utf-8'))
    parse_result = {"device_id": dic_payload["device_id"], "key": dic_payload["key"],
                    "value": dic_payload["value"]}
    dic_id_gateway[dic_payload["device_id"]] = dic_payload["gw_id"]
    print("Parse Result:", parse_result)
    return parse_result


def send_data(device_id, data):
    topic = generate_south_send_topic(device_id, "write")
    mqtt_south_send(topic, data)


def init():
    print("INFO(SouthCommunication MODULE): INIT BEGIN")
    global SUBSCRIBE_TOPIC, client_south, module_communicate_client
    # 设置南向客户端的回调函数
    client_south.on_connect = on_connect_south
    client_south.on_message = on_message_south
    client_south.on_publish = on_publish
    client_south.on_disconnect = on_disconnect
    # 配置南向客户端
    client_south.username_pw_set("mosquitto", "mosquitto")
    client_south.connect(SOUTH_HOST, PORT, KEEP_ALIVE)
    client_south.loop_start()

    # 注册模块内部通信
    module_communicate_client = MecInnerCommunication.module_register("SouthCommunication", message_hook, on_connect)
    module_communicate_client.subscribe("SouthCommunication/#", qos=0)
    time.sleep(1)
    print("INFO(SouthCommunication MODULE): INIT SUCCESS")


def on_connect(client, userdata, flags, rc):
    message = MecInnerCommunication.generate_mec_json('status', {"module": "SouthCommunication", "status": "on"},
                                                      "SouthCommunication")
    module_communicate_client.publish("MecInnerCommunication/service", str(message))
    print("Info(SouthCommunication MODULE REGISTER): Connect Success")


# 收到内部通信消息的回调函数
def message_hook(client, userdata, msg):
    print("Module SouthCommunicate Message Received")
    if str(msg.topic).split('/')[-1] == "service":
        service_message_handler(msg.payload)
    else:
        print("ERROR(SouthCommunicate MODULE): Unknown topic type, drop message" + str(msg.topic))


def service_message_handler(message):
    dic_message = eval(str(message, 'utf-8'))
    if dic_message["Type"] == "calculate.rep":
        parse_result = dic_message["Data"]
        try:
            topic = "BY2/D/" + parse_result["device_id"] + "/write"
            payload = {"type": "update", "device_id": parse_result["device_id"], "key": parse_result["key"],
                         "value": parse_result["value"]}
        except:
            print("ERROR(Module SouthCommunication): generate topic and message false, drop it")
            return

        print(topic, payload)
        client_south.publish(topic, str(payload))
    else:
        print("ERROR(DeviceManager MODULE): Unknown message type, drop message:" + dic_message["Type"])


if __name__ == '__main__':
    init()
    while True:
        print(dic_id_gateway)
        time.sleep(10)
