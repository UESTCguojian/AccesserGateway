# -*- coding:UTF-8 -*-

# author:郭健
# contact: guojian_emails@163.com
# datetime:2021/10/18 10:34
# software: PyCharm

"""
文件说明：
    实现MEC与设备之间的南向通信
    本文件假设为TCP
"""
import socket
import threading
import time


dic_address_connection = {}


def init(ip, port, connection_num):
    connection = get_connection(ip, port, connection_num)
    t = threading.Thread(target=listen_port, args=(connection,))
    t.setName("MainListenThread")
    t.start()


def get_connection(ip_input, port_input, connection_num_input):
    tcp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_server_socket.bind((ip_input, port_input))
    tcp_server_socket.listen(connection_num_input)
    return tcp_server_socket


def listen_port(connection_listen):
    sem = threading.Semaphore(3)
    while True:
        service, address_get = connection_listen.accept()
        print("Receive request:")
        print(address_get)
        t = threading.Thread(target=wait_request, args=(service, address_get, sem))
        t.setName(address_get[1])
        t.start()


def wait_request(service_instance, address, sem):
    with sem:
        while True:
            try:
                receive_data = service_instance.recv(1024).decode("utf-8")
                print(address, receive_data)
                dic_message = data_split(receive_data)
                dic_address_connection[dic_message["DeviceID"]] = service_instance
            except:
                print("close connection local")
                service_instance.close()
                dic_address_connection.pop(dic_message["DeviceID"])
                break


# todo: 这里需要根据不同的信息进行自定义消息处理函数
def data_split(receive_data):
    try:
        data = receive_data.split(",")
        dic_message = {"DeviceID": data[0].split(":")[1], "KeyName": data[1].split(":")[1], "Value": data[2].split(":")[1], "Type": data[3].split(":")[1]}
        return dic_message
    except:
        print("Data split error, please check message(example: id:by-001, keyname: humidity, value:10, type: update)")
        return None


def send_data(device_id, data):
    try:
        dic_address_connection[device_id].send(data.encode("utf-8"))
    except:
        print("south send error")

if __name__ == '__main__':
    print("South Communication Test")
    ip = "127.0.0.1"
    port = 7890
    connection_num = 5
    init(ip, port, connection_num)
    while True:
        print(dic_address_connection)
        time.sleep(5)
