import os
import json
import time
import sys
import getopt
import random
import logging
import threading
import queue 
import sqlite3
import paho.mqtt.client as mqtt
from Sqlite3toCSV import sqlite2csv,sqlite2html
from Sqlite3Adapter import SQL_data_adapter

q = queue.Queue()
last_message=dict()
## config managemnet
CM = dict()  

# Sqlite3 DataBase Config




# Persistent_Server Config
logging.basicConfig(level = logging.INFO) 

broker           = "127.0.0.1"
port             = 1883
verbose          = True
ServerName       = "PersistentMQTT_Server0" 
topics           = [("#",0)]  # select all topics
Username         = "22051153" # login 
Password         = "yixiao"   # login
synctime         = 1          # time to sync data form sqlite db to csv file
db_file          = "MsgStore.db"
Table_name       = "PersistentPage"
csv_name         =  db_file + ".csv"
table_fields     = {
                "id"        :"integer primary key autoincrement",
                "systime"  :"int",
                "msgtime"  :"float",
                "topic"     :"text",
                "message"   :"text",
                "qos"       :"int",
                }


def Persistent_Store_Handler(adapter,data):
    systime     = data["systime"]
    msgtime     = data["msgtime"]
    topic       = data["topic"]
    message     = data["message"]
    qos         = data["qos"]
    
    data_query  = "INSERT INTO "+ Table_name + "(systime,msgtime,topic,message,qos)VALUES(?,?,?,?,?)"   
    data_out    = [systime,msgtime,topic,message,qos]

    adapter.Log_client(data_query,data_out)
    
def DataBase_Sync_Handler():
    while True:
        time.sleep(synctime)
        sqlite2csv(db_file,Table_name,csv_name)
        print("DataBase Sync done!")

def Server_Main_Loop():
    #create adapter
    adapter = SQL_data_adapter(db_file)
    
    # 重新加载table
    adapter.drop_table("PersistentPage")
    adapter.create_table("PersistentPage",table_fields)

    while Server_On:
        while not q.empty():
            data  =  q.get()
            if data:
                try:
                    Persistent_Store_Handler(adapter,data)
                except Exception as e:
                    print("problem with logging ",e)
            else:
                continue
    adapter.conn.close()

            #print("message saved ",results["message"])


# A paho client and its APIs 
def on_connect(SC, userdata, flags, rc):
    print("Connected flags: "+ str(flags) +"\nresult code: "+str(rc))
    # handler
    if rc:
        SC.bad_connection_flag = True
    else:
        SC.connected_flag = True
        

def on_disconnect(SC, userdata, rc):
    print("disconnecting reason: " + str(rc))
    # handler
    SC.connected_flag   = False
    SC.disconnect_flag  = True
    SC.subscribe_flag   = False
    
def on_subscribe(SC,userdata,mid,granted_qos):
    print("In One subscribe callback\nresult: "+str(mid))
    # handler
    SC.subscribed_flag  = True

# ’timestamp', 'state', 'dup', 'mid', '_topic', 'payload', 'qos
def on_message(SC,userdata,msg):
    print("message received",msg)
    decoded_msg=msg.payload.decode("utf-8","ignore")
    print ("payload:",decoded_msg)
    print ("systime:",time.time())
    print ("msgtime:",msg.timestamp)
    print ("topic:"  ,msg._topic)
    print ("qos:"    ,msg.qos)
    # handler
    data = dict()
    data["systime"]     = int(time.time())
    data["msgtime"]     = float(msg.timestamp)
    data["topic"]       = msg.topic
    data["message"]     = str(msg.payload.decode("utf-8","ignore"))
    data["qos"]         = int(msg.qos)
    q.put(data)

# Register a Paho Client as a Server and listen to all the information of the broker
# 注册一个Paho Client作为Server，监听broker的所有信息
def Server_Init(ServerName, Username, Password):
    SC = mqtt.Client(ServerName)
    SC.on_connect       = on_connect        
    SC.on_message       = on_message        
    SC.on_disconnect    = on_disconnect
    SC.on_subscribe     = on_subscribe
    SC.username_pw_set(Username, Password)
    return SC

if __name__  ==  "__main__":
    print("Persistent Server Creating: ", ServerName)

    SC = Server_Init(ServerName, Username, Password)

    print("Server ", Username, "Connected to Broker ")    
    
    # Start adapter
    Server_On = True
    t0 = threading.Thread(target = Server_Main_Loop) 
    t0.start() 
    # another thread: save data base to csv file 
    t1 = threading.Thread(target = DataBase_Sync_Handler) 
    t1.start() 

    SC.connected_flag       = False 
    SC.bad_connection_flag  = False
    SC.subscribed_flag      = False

    SC.loop_start()
    # 
    SC.connect(broker,port)
    while not SC.connected_flag:  #wait for connection
        time.sleep(1)
    # 
    SC.subscribe(topics)
    while not SC.subscribed_flag: #wait for connection
        time.sleep(1)
    
    print("subscribed ",topics)


    # loop and wait until interrupted
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("interrrupted by keyboard")

    # Final check for messages
    SC.loop_stop()  
    time.sleep(5)
    
    # Stop logging thread
    Server_On = False 
    print("ending ")

