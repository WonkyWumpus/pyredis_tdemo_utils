# zset-and-hash.py
import redis
# import the pyredis_tdemo_utils module and then use the metaclass to redfine the Redis class
from pyredis_tdemo_utils import pyredistdemoutils
redis.Redis = pyredistdemoutils.RedisRetryAndMetics(
    redis.Redis.__name__, redis.Redis.__bases__, redis.Redis.__dict__)

# RedisCluster inherits all the capabilities of the redefined Redis
import rediscluster

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
#default_histo_list = [(50, 100), (100, 200), (200, 400), (400, 800), (1000, 575)]
default_histo_list = [(100, 100), (1000,10)]
# List of seconds to wait on each retry attempt
default_retry_list = [1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,5,5,5,5,5]

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", help="location of config file", default="zset-and-hash.yaml")
parser.add_argument("-c", "--clients", type=int, help="number of clients", default=1)
parser.add_argument("--hashKeyStart", type=int, help="starting key name for hashes, overrides config file")
parser.add_argument("--hashNumKeys", type=int, help="number of hash keys, overrides config file")
parser.add_argument("--clientName", help="name of client, shows in output", default="Client-Main")
parser.add_argument("--clusterMode", help="clusterMode=[true|false], true to use redis cluster API", choices=["true", "false"])
parser.add_argument("--opsPerSec", type=int, help="number of operations per second")
parser.add_argument("mode", help="[LOAD|QUERY], running in load or query mode")
args = parser.parse_args()

redisConn = ""
configs = {}
rcMain = None

def clientLog(logMessage):
    print(args.clientName + ": " + logMessage, flush=True)

def connToRedis():
    if (configs['redisConn']['clusterMode'] == "false"):
        return redis.Redis(
          host=configs['redisConn']['host'],
          port=configs['redisConn']['port'],
          password=configs['redisConn']['password'],
          socket_timeout=2)
    else:
        rcNodes = [{"host": configs['redisConn']['host'], "port": configs['redisConn']['port'], "password": configs['redisConn']['password']}]
        return rediscluster.RedisCluster(startup_nodes=rcNodes, decode_responses=True, ssl=False, password=configs['redisConn']['password'])

def loadZsetsAndHashes(rcIn):
    for keyNameInt in range (configs['hash']['keyStart'], configs['hash']['numKeys']+configs['hash']['keyStart']):
        keyName = configs['hash']['keyPrefix'] + str(keyNameInt).zfill(configs['hash']['keyNameLength'])

        hashElementCmd = {}

        for elementNameInt in range(configs['hash']['elementStart'], configs['hash']['numElements'] + 1):
            hashElementCmd[str(elementNameInt).zfill(configs['hash']['elementNameLength'])] = ''.join(
            random.choices(string.digits, k=configs['hash']['elementValueLength']))

        rcMain.hset(keyName, mapping=hashElementCmd)

        for zsetKeyNameInt in range(1, configs['zset']['numKeys']+1):
            zsetKeyName = configs['zset']['keyPrefix'] + str(zsetKeyNameInt).zfill(configs['zset']['keyNameLength'])
            # Score is calculated as the double of the first scoreLength digits in the value of the hash element
            score = hashElementCmd[str(zsetKeyNameInt).zfill(configs['hash']['elementNameLength'])][:configs['zset']['scoreLength']]
            rcMain.zadd(zsetKeyName, {keyName: score})

def queryZsetsAndHashes(rcIn):

    # Fetch minimum score in each zset the determine bound for zset queries
    zsetMins = {}
    if int(configs['zset']['numKeys'] > 0):
        for zsetKeyNameInt in range(1, configs['zset']['numKeys'] + 1):
            zsetKeyName = configs['zset']['keyPrefix'] + str(zsetKeyNameInt).zfill(configs['zset']['keyNameLength'])
            results = rcMain.zrangebyscore(zsetKeyName, 0, 9999999999999999999999999, start=0, num=1, withscores=True)
            # clientLog("Score bound: " + zsetKeyName + ': ' + str(results[0][1]))
            zsetMins[zsetKeyName] = results[0][1]

    # Loop exits based on the runtime parameter
    startMicros = microseconds()
    while ((microseconds() - startMicros) < configs['query']['runTimeSecs']*1000*1000):

        #Clunky if statement for the case when there are no zsets, will run as just a hash query in this case
        if int(configs['zset']['numKeys'] > 0):
            # Zset Query
            zsetKeyNameInt = random.randint(1, configs['zset']['numKeys'])
            zSetKeyName = configs['zset']['keyPrefix'] + str(zsetKeyNameInt).zfill(configs['zset']['keyNameLength'])
            zsetScoreQuery = random.randint(int(zsetMins[zsetKeyName]), int(str(configs['zset']['scoreLength'])*9))
            results = rcMain.zrevrangebyscore(zSetKeyName, zsetScoreQuery, 0, start=0, num=1, withscores=True, re_met_trans_id="ZREVRANGE-01")

            # Hash Query
            hashKeyName = results[0][0]
            results = rcMain.hgetall(hashKeyName, re_met_trans_id="HGETALL-01")

        else:
            # Hash Query
            hashKeyInt = random.randint(1, configs['hash']['numKeys'])
            hashKeyName = configs['hash']['keyPrefix'] + str(hashKeyInt).zfill(configs['hash']['keyNameLength'])
            results = rcMain.hgetall(hashKeyName, re_met_trans_id="HGETALL-01")


#
# For launching multiple clients.  SOme crude process handling, including scraping histogram output from the
#   subprocesses and making the required calls to calc and print out overall stats results.
#
def launchWorkers():

    remainingClients = args.clients
    remainingKeys = configs['hash']['numKeys']
    currentKeyStart = configs['hash']['keyStart']
    remainingOpsSec = configs['opsPerSec']

    myClients = []
    while remainingClients > 0:

        numKeysForClient = int(remainingKeys / remainingClients)
        opsPerSec = int(remainingOpsSec / remainingClients)
        subargs = [
            sys.executable,
            sys.argv[0],
            "--clientName=Client-" + str(remainingClients).zfill(5),
            "--hashKeyStart=" + str(currentKeyStart),
            "--hashNumKeys=" + str(numKeysForClient),
            "--opsPerSec=" + str(opsPerSec)
        ]

        if (args.clusterMode != None):
            subargs.append("--clusterMode=" + args.clusterMode)

        subargs.append(args.mode)

        print(args.clientName + ": Launching client:", subargs, flush=True)
        myClients.append(subprocess.Popen(subargs, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8', universal_newlines=True))
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

    if args.mode not in ["LOAD", "QUERY"]:
        clientLog("Invalid mode: " + args.mode)
        exit(1)

    with open(args.file, 'r') as stream:
        try:
          configs = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
          clientLog(exc)
          exit(1)

    if (args.hashKeyStart or (args.hashKeyStart == 0)):
        if (args.hashKeyStart < 1):
            clientLog("--hashKeyStart must be greater than 0")
            exit(1)
        configs['hash']['keyStart'] = args.hashKeyStart

    if (args.hashNumKeys or (args.hashNumKeys == 0)):
        if (args.hashNumKeys < 1):
            clientLog("--hashNumKeys must be greater than 0")
            exit(1)
        configs['hash']['numKeys'] = args.hashNumKeys

    if (configs['hash']['numElements'] < configs['zset']['numKeys']):
        clientLog('Error: Cannot have more zsets than keys in your hashes')
        exit(1)

    if (args.clients > configs['hash']['numKeys']):
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
        rcMain.re_met_init_trans(trans_id='ZREVRANGE-01', histo_list=default_histo_list)
        rcMain.re_met_init_trans(trans_id='HGETALL-01', histo_list=default_histo_list, trans_per_sec=configs['opsPerSec'])

        if (args.mode == "LOAD"):
            loadZsetsAndHashes(None)
        elif (args.mode == "QUERY"):
            queryZsetsAndHashes(None)
        else:
            clientLog("ERROR: Unknown mode")
            exit(1)
        rcMain.re_met_report_trans(trans_id='HGETALL-01')
        rcMain.re_met_output_full_histo()
    else:
        # os.environ["PYTHONUNBUFFERED"] = "1"
        launchWorkers()
        pyredistdemoutils.RedisRetryAndMeticsData.print_re_merged_trans_ids()


    #clientLog("SUCCESS: Exiting")
    exit(0)