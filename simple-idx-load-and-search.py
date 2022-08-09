# Simple test of the redis_retry_and_metrics using set and get in a tight loop
import redis
import argparse

from pyredis_tdemo_utils import pyredistdemoutils

# Bucket definition for histograms.  Format is [(<microseconds>,<numbuckets>), ...]
default_histo_list = [(50, 100), (100, 200), (200, 400), (400, 800), (1000, 575)]
#default_histo_list = [(100, 100), (1000,10)]

# List of seconds to wait on each retry attempt
default_retry_list = [1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,5,5,5,5,5]

def clientLog(logMessage):
    print(logMessage)

#####
# Main - Refactor to function
#####
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--server", help="host", default="localhost")
    parser.add_argument("-p", "--port", help="port", default=6379)
    parser.add_argument("-a", "--auth", help="password", default="")
    parser.add_argument("-i", "--iterations", type=int, default=100)
    parser.add_argument("-r", "--rate", help="rate in transactions per second", type=int, default=100)
    args = parser.parse_args()

    # Metaclass for injection of metrics and retry capabilities
    redis.Redis = pyredistdemoutils.RedisRetryAndMetics(redis.Redis.__name__, redis.Redis.__bases__, redis.Redis.__dict__)

    rc = redis.Redis(
        host=args.server,
        port=args.port,
        password=args.auth)

    # Initialize/name the transactions that you will be calling
    #   Please not, if you are using the trans_per_sec option to throttle you calls, you should only
    #   throttle ONE transaction in your loop
    rc.re_met_init_trans(trans_id='DELETE-IDX-01', histo_list=default_histo_list, retry_list=default_retry_list)
    rc.re_met_init_trans(trans_id='CREATE-IDX-01', histo_list=default_histo_list, retry_list=default_retry_list, trans_per_sec=args.rate)
    rc.re_met_init_trans(trans_id='INSERT-JSON-01', histo_list=default_histo_list, retry_list=default_retry_list)
    rc.re_met_init_trans(trans_id='SEARCH-IDX-01', histo_list=default_histo_list, retry_list=default_retry_list)

    for i in range(args.iterations):

         result = rc.execute_command('ft.dropindex', 'idx:001', 'DD', re_met_trans_id="DELETE-IDX-01");
         result = rc.execute_command(
           'ft.create', 'idx:001', 'on', 'JSON', 'prefix', '1', 'user:001', 'schema', 'tag001', 'TAG', 'text001', 'TEXT', 'numeric001', 'NUMERIC', re_met_trans_id="CREATE-IDX-01")
         result = rc.execute_command('json.set', 'user:001:001', "$", '{"tag001": "LEATHER", "text001": "Hello world", "numeric001": 7}', re_met_trans_id="INSERT-JSON-01");
         result = rc.execute_command('ft.search', 'idx:001', '@tag001:{LEATHER}', re_met_trans_id="SEARCH-IDX-01");

    # Print out metrics
    rc.re_met_report_trans()
