#!/usr/bin/env python3
#-*- coding: utf-8 -*-

# import the MongoClient class of the PyMongo library
import sys
import os
import time
from pymongo import MongoClient
#import argparse
import threading
import json
import platform

# Set "__location__" equal to the path to this script. Used to easily read the config file regardless of where this script is unzipped. 
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
all_okays = []
all_p_status = []
all_hosts = []
all_ports = []
all_users = []
all_passes = []
all_set_names = []
all_connections = []
default_connectTO = 4000
default_loopPeriod = 20
max_connectTO = 8000
min_connectTO = 1000
max_loopPeriod = 120
min_loopPeriod = 10
connectTO = 0
loopPeriod = 0
int_type = 1
string_type = "1"
maxThreads = 30
threadLimiter = threading.Semaphore(maxThreads)
useDefaultUser = True
useDefaultPass = True

use_cls = True
if platform.system() == "Windows":
    use_cls = True
else:
    use_cls = False

def main():
    global threadLimiter
    global maxThreads
    # Open JSON config file and validate format
    json_data = open(os.path.join(__location__, 'testconfig.json'))
    json_data.seek(0)
    isConfigFileValidJson = validateConfigFormat(json_data)
    if isConfigFileValidJson == False:
        print('Json config file is invalid - Not JSON format!\nExiting...')
        sys.exit(1)
    # Load JSON from config file, validate all Keys, and set all parameters
    json_data.seek(0)
    config = json.load(json_data)
    areConfigEntriesValid = validateSetConfigEntries(config)
    if areConfigEntriesValid == False:
        print('Config file invalid!\nExiting...')
        sys.exit(1)
    # Create the connections from the user, pass, host, and port lists
    callCreateConnections()
    # Get all of the Replica Set names
    callGetSetNames()
    # Wait for threads to finish
    while threadLimiter._value < maxThreads:
        time.sleep(1)
    # Clear SHELL and start queries
    clearSHELL()
    # Get the Stati
    threading.Thread(target=idleKillCounter).start()
    try:
        while True:
            performStatusQueries()
            while threadLimiter._value < maxThreads:
                time.sleep(1)
            printStatusResults()
            time.sleep(loopPeriod)
    except KeyboardInterrupt:
        print("Program terminated manually!\nExiting...")
        sys.exit(0)

def validateConfigFormat(json_in):
    try:
        test_config = json.load(json_in)
    except:
        print("Issue with config file...")
        return False
    return True

def validateSetConfigEntries(config_in):
    try:
        checkTimings(config_in)
        checkForHostsSection(config_in)
        checkHostsEntries(config_in)
    except ValueError as err:
        print("Issue with hosts section...\n")
        print(err)
        return False
    return True

def checkTimings(config_in):
    global loopPeriod
    global connectTO
    global default_loopPeriod
    global default_connectTO
    global max_loopPeriod
    global min_loopPeriod
    global max_connectTO
    global min_connectTO
    if "periodSeconds" in config_in:
        temp_period = config_in['periodSeconds']
        if type(temp_period) == type(int_type):
            if temp_period >= min_loopPeriod:
                if temp_period <= max_loopPeriod:
                    loopPeriod = config_in['periodSeconds']
                else:
                    loopPeriod = max_loopPeriod
            else:
                loopPeriod = min_loopPeriod
        else:
            raise ValueError('loopPeriod is not an integer!')
    else:
        loopPeriod = default_loopPeriod
    if "timeoutSeconds" in config_in:
        temp_timeout = config_in['timeoutSeconds']
        if type(temp_timeout) == type(int_type):
            if temp_timeout >= min_connectTO:
                if temp_timeout <= max_connectTO:
                    connectTO = config_in['timeoutSeconds'] * 1000
                else:
                    connectTO = max_connectTO
            else:
                connectTO = min_connectTO
        else:
            raise ValueError('timeoutSeconds is not an integer!')
    else:
        connectTO = default_connectTO

def checkForHostsSection(config_in):
    if not "hosts" in config_in:
        raise ValueError('Something is wrong with the hosts section!')

def checkHostsEntries(config_in):
    for x in config_in['hosts']:
        checkHostname(x)
        checkUsername(x, config_in)
        checkPassword(x, config_in)
        checkPorts(x, config_in)

def checkHostname(x_in):
    if not "hostname" in x_in:
        raise ValueError('One of the hosts entries is missing the hostname field!')
    else:
        if len(x_in['hostname']) == 0:
            raise ValueError('hostname fields cannot be empty!')
        if not type(x_in['hostname']) == type(string_type):
            raise ValueError('One of the hostnames is not a valid string!')

def checkUsername(x_in, config_in):
    global useDefaultUser
    useDefaultUser = False
    if "username" in x_in:
        if not len(x_in['username']) == 0:
            if type(x_in['username']) == type(string_type):
                useDefaultUser = False
            else:
                checkDefaultUsername(config_in)
        else:
            checkDefaultUsername(config_in)
    else:
        checkDefaultUsername(config_in)

def checkDefaultUsername(config_in):
    global useDefaultUser
    if "defaultUsername" in config_in:
        if not len(config_in['defaultUsername']) == 0:
            if type(config_in['defaultUsername']) == type(string_type):
                useDefaultUser = True
            else:
                raise ValueError('Default user is not a valid string and a hosts entry is missing a username!')
        else:
            raise ValueError('Default user was not provided and a hosts entry is missing a username!')
    else:
        raise ValueError('Default user is not in config file and a hosts entry is missing a username!')

def checkPassword(x_in, config_in):
    global useDefaultPass
    useDefaultPass = False
    if "password" in x_in:
        if not len(x_in['password']) == 0:
            if type(x_in['password']) == type(string_type):
                useDefaultPass = False
            else:
                checkDefaultPassword(config_in)
        else:
            checkDefaultPassword(config_in)
    else:
        checkDefaultPassword(config_in)

def checkDefaultPassword(config_in):
    global useDefaultPass
    if "defaultPassword" in config_in:
        if not len(config_in['defaultPassword']) == 0:
            if type(config_in['defaultPassword']) == type(string_type):
                useDefaultPass = True
            else:
                raise ValueError('Default password is not a valid string and a hosts entry is missing a password!')
        else:
            raise ValueError('Default password was not provided and a hosts entry is missing a password!')
    else:
        raise ValueError('Default password is not in config file and a hosts entry is missing a password!')

def checkPorts(x_in, config_in):
    if not "ports" in x_in:
        raise ValueError('One of the host entries does not have a ports list!')
    if len(x_in['ports']) == 0:
            raise ValueError('One of the ports lists is empty!')
    for y in x_in['ports']:
        if not type(y) == type(int_type):
            raise ValueError('One of the ports lists contains a non-integer value!')
        else:
            all_hosts.append(x_in['hostname'])
            all_ports.append(str(y))
            if useDefaultUser == True:
                all_users.append(config_in['defaultUsername'])
            else:
                all_users.append(x_in['username'])
            if useDefaultPass == True:
                all_passes.append(config_in['defaultPassword'])
            else:
                all_passes.append(x_in['password'])

def callCreateConnections():
    global all_users
    global all_passes
    global all_hosts
    global all_ports
    global all_connections
    global all_okays
    global all_p_status
    # Create the connections from the user, pass, host, and port lists
    for (a, b, c, d) in zip(all_users, all_passes, all_hosts, all_ports):
        all_connections.append(createConnection(a, b, c, d))
        all_okays.append("")
        all_p_status.append("")

def createConnection(in_user_cc, in_pass_cc, in_host_cc, in_port_cc):
    # Create the connection string
    global connectTO
    mongo_con = 'mongodb://' + in_user_cc + ':' + in_pass_cc + '@' + in_host_cc + ':' + in_port_cc + '/?maxPoolSize=20&w=majority'
    mongo_client = MongoClient(mongo_con, connectTimeoutMS=connectTO, socketTimeoutMS=connectTO, serverSelectionTimeoutMS=connectTO)
    mongo_admin_cc = mongo_client["admin"]
    return(mongo_admin_cc)

def callGetSetNames():
    global all_connections
    global threadLimiter
    for idx, i in enumerate(all_connections):
        threadLimiter.acquire()
        threading.Thread(target=getSetNames, args=[i, idx]).start()

def getSetNames(mongo_admin_gsn, position_gsn):
    global all_set_names
    global threadLimiter
    try:
        this_set_name = mongo_admin_gsn.command("serverStatus")["repl"]["setName"]
        all_set_names.append(this_set_name)
    except:
        all_set_names.append('nothing')
    finally:
        threadLimiter.release()

def performStatusQueries():
    global threadLimiter
    for idx, i in enumerate(all_connections):
        position_psq = idx
        threadLimiter.acquire()
        threading.Thread(target=getStatus1, args=[i, position_psq]).start()
        threadLimiter.acquire()
        threading.Thread(target=getStatus2, args=[i, position_psq]).start()

def getStatus1(mongo_admin_gs1, position_gs1):
    global all_okays
    global threadLimiter
    try:
        this_okay = mongo_admin_gs1.command("serverStatus")["ok"]
        all_okays[position_gs1] = (this_okay)
    except:
        all_okays[position_gs1] = (-1)
    finally:
        threadLimiter.release()

def getStatus2(mongo_admin_gs2, position_gs2):
    global all_p_status
    global threadLimiter
    try:
        this_ps = mongo_admin_gs2.command("serverStatus")["repl"]["isWritablePrimary"]
        all_p_status[position_gs2] = (this_ps)
    except:
        all_p_status[position_gs2] = ('ERROR')
    finally:
        threadLimiter.release()

def printStatusResults():
    global threadLimiter
    global all_hosts
    global all_ports
    global all_set_names
    global all_okays
    global all_p_status
    # Print Statis results...
    clearSHELL()
    print("%-20s %-10s %-15s %-8s %-12s" % ("Host:", "Port:", "RSName:", "Okay:", "Primary:"))
    for (a, b, c, d, e) in zip(all_hosts, all_ports, all_set_names, all_okays, all_p_status):
        print("%-20s %-10s %-15s %-8s %-12s" % (a, b, c, d, e))

def clearSHELL():
    global use_cls
    if use_cls:
        os.system('CLS')
    else:
        os.system('CLEAR')

def idleKillCounter():
    time.sleep(14400)
    print("\nRun-Time Limit Exceeded.\nExiting...")
    sys.exit(0)

if __name__ == "__main__":
    main()
