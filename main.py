# -*- coding:UTF-8 -*-

# author:郭健
# contact: guojian_emails@163.com
# datetime:2021/10/20 20:32
# software: PyCharm

"""
文件说明：
    
"""
import time

import DeviceManager
import MecInnerCommunication
import MessageManager
import ModuleManager
import SouthCommunication


def init():
    MecInnerCommunication.init()
    SouthCommunication.init()
    ModuleManager.init()
    MessageManager.init()
    DeviceManager.init()
    while True:
        print(MecInnerCommunication.module_list)
        time.sleep(10)


if __name__ == '__main__':
    init()
