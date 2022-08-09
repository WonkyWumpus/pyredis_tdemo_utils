#
# Redis Retry and Metrics
#   Wrapper for redis and rediscluster that adds configurable retry, throttle, and metrics
#

import array
import random
from time import time, sleep
from types import FunctionType
from functools import wraps
import ast

class PyRedisTdemoUtils:

    def __init__(self, redis):
        super().__init__(self,redis)
        # self.redis = redis
        # self.rediscluster = redis
        self.named_transactions = []

    def retry(self, func):

        from types import FunctionType
        from functools import wraps

#
# wrapper
#   Used to decorate all existing functions in Redis and inject timing and retry logic
def wrapper(method):
    @wraps(method)
    def wrapped(*args, **kwrds):

        re_met_trans_id = None
        new_kwrds = {}
#        if (method.__name__ not in ['execute_command', 'parse_response', '__init__', '__del__'] and (kwrds is not None)):
        if (method.__name__ not in ['parse_response', '__init__', '__del__'] and (kwrds is not None)):
            # print('Custom Logic: ' + method.__name__)
            new_kwrds = {}
            for key, value in kwrds.items():
                if (key == 're_met_trans_id'):
                    # print(value)
                    re_met_trans_id = value
                else:
                    new_kwrds[key] = value

            if re_met_trans_id:
                # start timer
                args[0].re_met_start_trans(re_met_trans_id)
                #
                # Adding retry logic with downtime reporting
                #
                for i in [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2]:
                    try:
                        results = method(*args, **new_kwrds)
                        break
                    except TimeoutError:
                        print(TimeoutError)
                    except ConnectionError:
                        print(ConnectionError)
                    except Exception as e:
                        print("Some other exception: " + e)
                    print("RETRY")
                    sleep(i)
            else:
                results = method(*args, **new_kwrds)

            if re_met_trans_id:
                # end timer
                args[0].re_met_end_trans(re_met_trans_id)

            return results
        else:
            results = method(*args, **kwrds)
            return results

    return wrapped

#
# RedisRetryAndMeticsData class is used to hold and manipulate some data outside of the Redis instance
#   Most notably, class attributes and methods are used to summarize metrics from different runs.
#   It is up to the calling programs to call these methods in order to do the sumarization.
class RedisRetryAndMeticsData(object):
    re_met_trans_ids = {}
    re_merged_trans_ids = {}

    @classmethod
    def add_merged_trans_id(cls, in_histo_data):
        if (in_histo_data['TransId'] in cls.re_merged_trans_ids.keys()):
            #logic to merge matching transactions
            # print("###MERGING###)")
            # print(in_histo_data)
            cls.re_merged_trans_ids[in_histo_data['TransId']]['count'] = \
                cls.re_merged_trans_ids[in_histo_data['TransId']]['count'] + in_histo_data['count']
            cls.re_merged_trans_ids[in_histo_data['TransId']]['totalMicroS'] = \
                cls.re_merged_trans_ids[in_histo_data['TransId']]['totalMicroS'] + in_histo_data['totalMicroS']
            cls.re_merged_trans_ids[in_histo_data['TransId']]['avgMicroS'] = \
                int(cls.re_merged_trans_ids[in_histo_data['TransId']]['totalMicroS'] / cls.re_merged_trans_ids[in_histo_data['TransId']]['count'])
            cls.re_merged_trans_ids[in_histo_data['TransId']]['minMicroS'] = \
                min(cls.re_merged_trans_ids[in_histo_data['TransId']]['minMicroS'], in_histo_data['minMicroS'])
            cls.re_merged_trans_ids[in_histo_data['TransId']]['maxMicroS'] = \
                max(cls.re_merged_trans_ids[in_histo_data['TransId']]['maxMicroS'], in_histo_data['maxMicroS'])
            myarraylen = len(cls.re_merged_trans_ids[in_histo_data['TransId']]['histoData'])
            for i in range(0, myarraylen - 1):
                cls.re_merged_trans_ids[in_histo_data['TransId']]['histoData'][i] = \
                    cls.re_merged_trans_ids[in_histo_data['TransId']]['histoData'][i] + in_histo_data['histoData'][i]
        else:
            # print("###ADDING###")
            # print(in_histo_data)
            cls.re_merged_trans_ids[in_histo_data['TransId']] = in_histo_data

    @classmethod
    def print_re_merged_trans_ids(cls, percentiles=[90, 95, 99, 99.5, 99.9]):

        for key in cls.re_merged_trans_ids:
            print("ALL TransId: " + key, end='')
            print(", count: " + str(cls.re_merged_trans_ids[key]['count']), end='')
            if (cls.re_merged_trans_ids[key]['count'] is not None):
                print(", totalMicroS: " + str(cls.re_merged_trans_ids[key]['totalMicroS']), end='')
                print(", avgMicroS: " + str(int(cls.re_merged_trans_ids[key]['totalMicroS'] / (
                cls.re_merged_trans_ids[key]['count']))), end='')
                print(", minMicroS: " + str(cls.re_merged_trans_ids[key]['minMicroS']), end='')
                print(", maxMicroS: " + str(cls.re_merged_trans_ids[key]['maxMicroS']), end='')

                histotranscount = 0
                histocountedbuckets = 0
                histocountedmicros = 0
                computedpercentiles = {}
                for percentile in percentiles:
                    computedpercentiles[percentile] = None

                for buckets in cls.re_merged_trans_ids[key]['histoList']:
                    for i in range(buckets[1]-1):
                        histotranscount = histotranscount + \
                                          cls.re_merged_trans_ids[key]['histoData'][histocountedbuckets]
                        histocountedmicros = histocountedmicros + buckets[0]
                        histocountedbuckets = histocountedbuckets + 1

                        for percents in percentiles:
                            if ((not computedpercentiles[percents]) and (histotranscount >= (
                                    (percents / 100) * cls.re_merged_trans_ids[key]['count']))):
                                computedpercentiles[percents] = histocountedmicros

                print(", histoCnt: " + str(histotranscount), end='')
                for key in computedpercentiles:
                    print(", p" + str(key).replace('.', '') + ": " + str(computedpercentiles[key]), end='')
            print(flush=True)

#
# RedisRetryAndMetrics
#   Metaclass used to refinde Redis.  New methods for initailizing transactions, etc
class RedisRetryAndMetics(type):
    def __new__(meta, classname, bases, classDict):
        newClassDict = {}

        def re_met_none_sum(self, a, b):
            if a is None:
                return b
            else:
                return a + b

        def re_met_start_trans(self, trans_id):
            if self.re_met_trans_ids[trans_id]['startMicros'] is None:
                # Randomize start of first transaction
                sleepMicros = random.randint(0,RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['micros_per_trans'] or 2000)/(1000*1000)
            elif RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['micros_per_trans'] is not None:
                # Calculate sleep based on last sleep time
                sleepMicros = RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['micros_per_trans'] - \
                              ((int(time() * 1000 * 1000) - self.re_met_trans_ids[trans_id]['startMicros']))
            else:
                # No target trans per second, run without sleeping
                sleepMicros = 0
            if (sleepMicros > 25):
                sleep(sleepMicros/1000000)

            self.re_met_trans_ids[trans_id]['startMicros'] = int(time() * 1000 * 1000)

        def re_met_end_trans(self, trans_id):

            self.re_met_trans_ids[trans_id]['lastCompleteTime'] = int(time() * 1000 * 1000)
            self.currTransMicros = (self.re_met_trans_ids[trans_id]['lastCompleteTime']) - self.re_met_trans_ids[trans_id]['startMicros']

            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['minMicros'] =\
                min(filter(lambda x: x is not None, [RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['minMicros'], self.currTransMicros]))
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['maxMicros'] =\
                max(filter(lambda x: x is not None, [RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['maxMicros'], self.currTransMicros]))
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['totalMicros'] =\
                self.re_met_none_sum(RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['totalMicros'], self.currTransMicros)
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['transCount'] =\
                self.re_met_none_sum(RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['transCount'], 1)

            countedmicros = 0
            countedbuckets = 0
            for buckets in RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['histo_list']:
                # clientLog('Histo compute: ' + str(opMicros) + ', ' + str(countedMicros) + ', ' + str(countedBuckets) + ', ' + str(buckets[0]) + ', ' + str(buckets[1]))
                if (self.currTransMicros - countedmicros) < (buckets[0] * buckets[1]):
                    bucketposition = int((self.currTransMicros - countedmicros) / buckets[0])
                    RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['histo_array'][countedbuckets + bucketposition] = RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['histo_array'][countedbuckets + bucketposition] + 1
                    # clientLog("Wrote bucket " + str(countedBuckets2 + bucketPosition))
                    break
                else:
                    countedmicros = countedmicros + (buckets[0] * buckets[1])
                    countedbuckets = countedbuckets + buckets[1]

        def re_met_init_trans(*args, trans_id='NONE', histo_list=[], retry_list=[], trans_per_sec=None):

            args[0].re_met_trans_ids[trans_id] = {}
            args[0].re_met_trans_ids[trans_id]['lastCompleteTime'] = None
            args[0].re_met_trans_ids[trans_id]['startMicros'] = None
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id] = {}
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['totalMicros'] = None
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['transCount'] = None
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['histo_list'] = histo_list
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['retry_list'] = retry_list
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['minMicros'] = None
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['maxMicros'] = None
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['histoTotalBuckets'] = 0
            for buckets in histo_list:
                RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['histoTotalBuckets'] = \
                    RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['histoTotalBuckets'] + buckets[1]
            RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['histo_array'] = \
                array.array('l', (0 for i in range(0, RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['histoTotalBuckets'])))
            if (trans_per_sec is not None):
                RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['micros_per_trans'] = int((1/trans_per_sec)*1000*1000)
                # print("MICROS_PER_TRANS: " + str(RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['micros_per_trans']))
            else:
                RedisRetryAndMeticsData.re_met_trans_ids[trans_id]['micros_per_trans'] = None

        # Specialized dataline in output.  Formatted to be easily converted to a dict in calling program for further processing
        def re_met_output_full_histo(self):
            for key in RedisRetryAndMeticsData.re_met_trans_ids:
                if (RedisRetryAndMeticsData.re_met_trans_ids[key]['transCount'] is not None):
                    print("RE_MET_HISTO_DATA : {", end='')
                    print("'TransId': '" + key + "'", end='')
                    print(", 'count': " + oct(RedisRetryAndMeticsData.re_met_trans_ids[key]['transCount']), end='')
                    print(", 'totalMicroS': " + oct(RedisRetryAndMeticsData.re_met_trans_ids[key]['totalMicros']), end='')
                    print(", 'avgMicroS': " + oct(int(RedisRetryAndMeticsData.re_met_trans_ids[key]['totalMicros']/(RedisRetryAndMeticsData.re_met_trans_ids[key]['transCount']))), end='')
                    print(", 'minMicroS': " + oct(RedisRetryAndMeticsData.re_met_trans_ids[key]['minMicros']), end='')
                    print(", 'maxMicroS': " + oct(RedisRetryAndMeticsData.re_met_trans_ids[key]['maxMicros']), end='')
                    print(", 'histoList': " + str(RedisRetryAndMeticsData.re_met_trans_ids[key]['histo_list']), end='')
                    print(", 'histoData': [", end='')
                    myarraylen = len(RedisRetryAndMeticsData.re_met_trans_ids[key]['histo_array'])
                    for i in range (0, myarraylen-1):
                        print(oct(RedisRetryAndMeticsData.re_met_trans_ids[key]['histo_array'][i]), end='')
                        if (i < myarraylen-2):
                            print(',', end='')
                    print("]", end='', flush=True)
                    print("}", flush=True)


        def re_met_report_trans(*args, trans_id='ALL', percentiles=[90, 95, 99, 99.5, 99.9]):

            for key in RedisRetryAndMeticsData.re_met_trans_ids:
                print("TransId: " + key, end='')
                print(", count: " + str(RedisRetryAndMeticsData.re_met_trans_ids[key]['transCount']), end='')
                if (RedisRetryAndMeticsData.re_met_trans_ids[key]['transCount'] is not None):
                    print(", totalMicroS: " + str(RedisRetryAndMeticsData.re_met_trans_ids[key]['totalMicros']), end='')
                    print(", avgMicroS: " + str(int(RedisRetryAndMeticsData.re_met_trans_ids[key]['totalMicros']/(RedisRetryAndMeticsData.re_met_trans_ids[key]['transCount']))), end='')
                    print(", minMicroS: " + str(RedisRetryAndMeticsData.re_met_trans_ids[key]['minMicros']), end='')
                    print(", maxMicroS: " + str(RedisRetryAndMeticsData.re_met_trans_ids[key]['maxMicros']), end='')

                    histotranscount = 0
                    histocountedbuckets = 0
                    histocountedmicros = 0
                    computedpercentiles = {}
                    for percentile in percentiles:
                        computedpercentiles[percentile] = None

                    for buckets in RedisRetryAndMeticsData.re_met_trans_ids[key]['histo_list']:
                        for i in range(buckets[1]-1):
                            histotranscount = histotranscount + RedisRetryAndMeticsData.re_met_trans_ids[key]['histo_array'][histocountedbuckets]
                            histocountedmicros = histocountedmicros + buckets[0]
                            histocountedbuckets = histocountedbuckets + 1

                            for percents in percentiles:
                                if ((not computedpercentiles[percents]) and (histotranscount >= ((percents/100) * RedisRetryAndMeticsData.re_met_trans_ids[key]['transCount']))):
                                    computedpercentiles[percents] = histocountedmicros

                    print(", histoCnt: " + str(histotranscount), end='')
                    for key in computedpercentiles:
                        print(", p" + str(key).replace('.','') + ": " + str(computedpercentiles[key]), end='')
                print()

        # Metaclass attribute building
        for attributeName, attribute in classDict.items():
            if isinstance(attribute, FunctionType):
                # replace it with a wrapped version
                attribute = wrapper(attribute)
            newClassDict[attributeName] = attribute
        newClassDict[re_met_init_trans.__name__] = re_met_init_trans
        newClassDict[re_met_start_trans.__name__] = re_met_start_trans
        newClassDict[re_met_end_trans.__name__] = re_met_end_trans
        newClassDict[re_met_none_sum.__name__] = re_met_none_sum
        newClassDict[re_met_report_trans.__name__] = re_met_report_trans
        newClassDict[re_met_output_full_histo.__name__] = re_met_output_full_histo
        newClassDict['re_met_trans_ids'] = {}
        newClassDict['currTransMicros'] = None
        return type.__new__(meta, classname, bases, newClassDict)