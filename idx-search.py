#!/usr/local/bin/python3
# idx-search.py
import redis
import os
# import the pyredis_tdemo_utils module and then use the metaclass to redfine the Redis class
from pyredis_tdemo_utils import pyredistdemoutils
redis.Redis = pyredistdemoutils.RedisRetryAndMetics(
    redis.Redis.__name__, redis.Redis.__bases__, redis.Redis.__dict__)

# RedisCluster inherits all the capabilities of the redefined Redis

import yaml
import string
import random
import argparse
from time import (time)
import subprocess
import sys
from select import select
import ast

# Bucket definition for histograms.  Format is [(<microseconds>,<numbuckets>), ...]
default_histo_list = [(50, 100), (100, 200), (200, 400), (400, 800), (1000, 575)]
#default_histo_list = [(100, 100), (1000,10)]
# List of seconds to wait on each retry attempt
default_retry_list = [1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,5,5,5,5,5]

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", help="location of config file", default="idx-search.yaml")
parser.add_argument("-c", "--clients", type=int, help="number of clients", default=1)
parser.add_argument("--idxKeyStart", type=int, help="starting key name, overrides config file")
parser.add_argument("--idxNumKeys", type=int, help="number of idx keys, overrides config file")
parser.add_argument("--clientName", help="name of client, shows in output", default="Client-Main")
parser.add_argument("--clusterMode", help="clusterMode=[true|false], true to use redis cluster API", choices=["true", "false"])
parser.add_argument("--opsPerSec", type=int, help="number of operations per second")
args = parser.parse_args()

redisConn = ""
configs = {}
rcMain = None
global redis_pool

def clientLog(logMessage):
    print(args.clientName + ": " + logMessage, flush=True)

def connToRedis():
    if (configs['redisConn']['clusterMode'] == "false"):

        print("PID %d: initializing redis pool..." % os.getpid())
        redis_pool = redis.ConnectionPool(
          host=configs['redisConn']['host'],
          port=configs['redisConn']['port'],
          password=configs['redisConn']['password'],
          socket_timeout=2)

        return redis.Redis(connection_pool=redis_pool)
    else:
        exit(1) ;

def runSearch(rcIn):


    startMicros = microseconds()
    while ((microseconds() - startMicros) < configs['runTimeSecs']*1000*1000):

        for keyNameInt in range (configs['idx']['keyStart'], configs['idx']['numKeys']+configs['idx']['keyStart']):

            if ((microseconds() - startMicros) > configs['runTimeSecs']*1000*1000):
                break;

            keyName = "{" + configs['idx']['keyPrefix'] + str(keyNameInt).zfill(configs['idx']['keyNameLength']) + "}"

            result = rcMain.execute_command('ft.create', keyName, 'on', 'JSON', 'prefix', '1', 'doc:' + keyName, 'schema', 'tag001', 'TAG', 'text001', 'TEXT', 'numeric001', 'NUMERIC', re_met_trans_id="IDX-001");
            result = rcMain.execute_command('json.set', 'doc:' + keyName + ':' + '001', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="DOC-001");
            result = rcMain.execute_command('json.set', 'doc:' + keyName + ':' + '002', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="DOC-001");
            result = rcMain.execute_command('json.set', 'doc:' + keyName + ':' + '003', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="DOC-001");
            result = rcMain.execute_command('json.set', 'doc:' + keyName + ':' + '004', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="DOC-001");
            result = rcMain.execute_command('json.set', 'doc:' + keyName + ':' + '005', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="DOC-001");
            result = rcMain.execute_command('json.set', 'doc:' + keyName + ':' + '006', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="DOC-001");
            result = rcMain.execute_command('json.set', 'doc:' + keyName + ':' + '007', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="DOC-001");
            result = rcMain.execute_command('json.set', 'doc:' + keyName + ':' + '008', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="DOC-001");
            result = rcMain.execute_command('json.set', 'doc:' + keyName + ':' + '009', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="DOC-001");
            result = rcMain.execute_command('json.set', 'doc:' + keyName + ':' + '010', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="DOC-001");
            result = rcMain.execute_command('ft.search', keyName, '@tag001:{LEATHER}', re_met_trans_id="SEARCH-001");
#           result = rcMain.execute_command('ft.dropindex', keyName, 'DD');

#
# For launching multiple clients.  SOme crude process handling, including scraping histogram output from the
#   subprocesses and making the required calls to calc and print out overall stats results.
#
def launchWorkers():

    remainingClients = args.clients
    remainingKeys = configs['idx']['numKeys']
    currentKeyStart = configs['idx']['keyStart']
    remainingOpsSec = configs['opsPerSec']

    myClients = []
    while remainingClients > 0:

        numKeysForClient = int(remainingKeys / remainingClients)
        opsPerSec = int(remainingOpsSec / remainingClients)
        subargs = [
            sys.executable,
            sys.argv[0],
            "--clientName=Client-" + str(remainingClients).zfill(5),
            "--idxKeyStart=" + str(currentKeyStart),
            "--idxNumKeys=" + str(numKeysForClient),
            "--opsPerSec=" + str(opsPerSec)
        ]

        if (args.clusterMode != None):
            subargs.append("--clusterMode=" + args.clusterMode)

        print(args.clientName + ": Launching client:", subargs, flush=True)
        myClients.append(subprocess.Popen(subargs, bufsize=0, stdout=subprocess.PIPE, stderr=sys.stderr, encoding='utf-8', universal_newlines=True))
        remainingClients = remainingClients - 1
        remainingKeys = remainingKeys - numKeysForClient
        currentKeyStart = currentKeyStart + numKeysForClient
        remainingOpsSec = remainingOpsSec - opsPerSec

    print(args.clientName + " INFO: Waiting on clients")

    while myClients:
        # Remove finished processes from the list
        for p in myClients:
            if p.poll() is not None:  # process ended
                for l in p.stdout:
                    if (l[0: 17] == "RE_MET_HISTO_DATA"):
                        myDict = ast.literal_eval(l[20:])
                        pyredistdemoutils.RedisRetryAndMeticsData.add_merged_trans_id(myDict)
                    else:
                        print(l, end='', flush=True)
                p.stdout.close()  # clean up file descriptors
                myClients.remove(p)  # remove the process


#                (out, err) = p.communicate(timeout=.1)
#                if out:
#                   print("WARNING: Maybe missed worker output")
#                if err:
#                    print("WARNING: Maybe missed worker error output")
#                    print(err)

        # Attempt to read stdout where we can
        rlist = select([p.stdout for p in myClients], [], [], .01)[0]
        for f in rlist:
            l = None
            l = f.readline()
            if (l[0 : 17] == "RE_MET_HISTO_DATA"):
                myDict = ast.literal_eval(l[20:])
                pyredistdemoutils.RedisRetryAndMeticsData.add_merged_trans_id(myDict)
            else:
                print(l, end='', flush=True)

def validateArgs():

    with open(args.file, 'r') as stream:
        try:
          configs = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
          clientLog(exc)
          exit(1)

    if (args.idxKeyStart or (args.idxKeyStart == 0)):
        if (args.idxKeyStart < 1):
            clientLog("--idxKeyStart must be greater than 0")
            exit(1)
        configs['idx']['keyStart'] = args.idxKeyStart

    if (args.idxNumKeys or (args.idxNumKeys == 0)):
        if (args.idxNumKeys < 1):
            clientLog("--idxNumKeys must be greater than 0")
            exit(1)
        configs['idx']['numKeys'] = args.idxNumKeys

    if (args.clients > configs['idx']['numKeys']):
        clientLog("Error: Cannot have fewer keys than clients")
        exit(1)

    if (args.clusterMode != None) :
        configs['redisConn']['clusterMode'] = args.clusterMode

    if (args.opsPerSec != None):
        configs['opsPerSec'] = args.opsPerSec

    return configs

microseconds = lambda: int(time() * 1000 * 1000)

#####
# Main - Refactor to function
#####
if __name__ == "__main__":
    clientLog("INFO: Entering client")
    configs = validateArgs()
    clientLog("CONFIG: " + str(configs))

    if (args.clients == 1):

        rcMain = connToRedis()
        # Only throttling one transaction in the loop, since it will naturally cause the other to be throttled
        #   throttle last trans, since the zero zset key setting skips the zset query
        rcMain.re_met_init_trans(trans_id='SEARCH-001', histo_list=default_histo_list)
        rcMain.re_met_init_trans(trans_id='DOC-001', histo_list=default_histo_list)
        rcMain.re_met_init_trans(trans_id='IDX-001', histo_list=default_histo_list, trans_per_sec=configs['opsPerSec'])

        runSearch(None)

        rcMain.re_met_report_trans(trans_id='SEARCH-001')
        rcMain.re_met_output_full_histo()
    else:
        # os.environ["PYTHONUNBUFFERED"] = "1"
        launchWorkers()
        pyredistdemoutils.RedisRetryAndMeticsData.print_re_merged_trans_ids()


    #clientLog("SUCCESS: Exiting")
    exit(0)