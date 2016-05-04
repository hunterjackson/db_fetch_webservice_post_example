import requests
import pypyodbc
import configparser
import logging
import datetime

from time import time
from time import sleep

"""
Basic idea behind this to query a db do some minimal parsing and then via a rest webservice call post the data to a
server elsewhere.

This script is just an example and was not based on an really data or servers

The fictional DB table named 'DATA' is configured as shown below

ID      GUIID      MACHINE_ID        PRODUCTION_RATE       EPOCH_TIME
 1      ABC1234     machine1            25                  4864382
 2      DEF789A     machine2            14                  4864389
"""


def post_data(e_point):
    """
    :param e_point: => the url of the endpoint to post the message too
    :return: boolean True => received 200 response else False
    """

    try:
        # post data to rest endpoint
        payload = {'time_since_epoch': row[4], 'widgets_per_sec': row[3]}
        response = requests.post(e_point, payload)

        if response.status_code == requests.codes.ok:
            logging.debug(response.text)
        else:
            logging.error('Post failed on endpoint=' + e_point + " with status code " +
                          response.status_code + ' response message=' + response.text)

        return True
    # normally a terrible practice but in this case if it doesn't work
    # I just want it to continue as fast as possible I don't care why
    except:
        logging.error("Failed to post to endpoint=" + e_point)
        return False


config = configparser.ConfigParser()  # setup configparser

# configure logging
logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', filename='server_' + str(datetime.date.today()) +
                                                                             '.log', level=logging.INFO)
logging.info('Server started')

while True:

    # I have this info inside the outer most while loop in case any DB server info needs to be changed or the urls
    # need to be changed then just the config file can be changed and there is no need to stop and start
    # this script again.

    # read in data from config.ini
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

    # endpoints
    endpoint1 = config['WEBSERVICE ENDOINTS']['endpoint1']
    endpoint2 = config['WEBSERVICE ENDOINTS']['endpoint2']

    logging.info("Info read in from config.ini pause_time=" + pause_time + " reconn_time=" + reconn_time + " driver=" +
                 driver + " server=" + server + ":" + port + " database=" + database + " endpoint1=" + endpoint1 +
                 "endpoint2=" + endpoint2)

    db_conn = None
    while db_conn is None:
        try:
            db_conn = pypyodbc.connect("Driver={" + driver + "};Server=" + server + "," +
                                       port + ";Database=" + database + ";uid=" +
                                       username + ";pwd=" + password + ";")
            curs = db_conn.cursor()

        except pypyodbc.Error:
            logging.warning('Failed to Connect to Database at ' + server + ":" + port)
            sleep(int(reconn_time))

    while True:
        # put sleep at the beginning in the case that the this app and
        # the server are started at the same time
        sleep(int(pause_time))

        # attempt db query
        try:
            curs.execute("SELECT * FROM DATA WHERE TIME_SINCE_EPOCH > " + str(time() - pause_time))
            data = curs.fetchall()  # put query in data variable
        except pypyodbc.Error:
            logging.error("Failed to execute sql statement will attempt reconnection")
            db_conn.close()
            break  # if cannot execute sql command assume connection has been broken break for reconnection

        # minimal parsing and send data to foreign server, picking endpoint based on data
        for row in data:
            if row[2] == 'machine1':
                res = post_data(endpoint1)

            elif row[2] == 'machine2':
                res = post_data(endpoint2)

            else:
                res = True  # handles the case if machinery is not machine 1 or machine 2

            if res is False:
                break  # if the web address cannot be posted to break and reread config file in hopes of a change
