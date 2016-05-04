import requests
import pypyodbc
import configparser
import logging
from time import sleep

config = configparser.ConfigParser()

config.read('config.ini')
pause_time = config['TIME']['pause_time']
reconn_time = config['TIME']['reconnect_time']

# SQL server settings
driver = config['DB INFO']['driver']
server = config['DB INFO']['server']
port = config['DB INFO']['port']
database = config['DB INFO']['database']
username = config['DB INFO']['username']
password = config['DB INFO']['password']

db_conn = None
while db_conn is None:
    try:
        db_conn = pypyodbc.connect("Driver={"+ driver +"};Server="+ server +"," +
                                    port + ";Database=" + database + ";uid=" +
                                    username + ";pwd=" + password + ";")
    except pypyodbc.Error:
        print("DB not connected")
        sleep(int(reconn_time))

while True:
    # put sleep at the beginning in the case that the this app and
    # the server are started at the same time
    sleep(int(pause_time))
