import redis
import rediscluster
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--server", help="host", default="localhost")
parser.add_argument("-p", "--port", help="port", default=6379)
parser.add_argument("-a", "--auth", help="password", default="")
parser.add_argument("--startKey", type=int, help="password", default="")
parser.add_argument("--num", type=int, help="password", default="")
parser.add_argument("--keyPrefix", help="password", default="")
parser.add_argument("--keyNameLength", type=int, help="password", default="")
args = parser.parse_args()

rcNodes = [{"host": args.server, "port": args.port, "password": args.auth}]
rc = rediscluster.RedisCluster(startup_nodes=rcNodes, decode_responses=True, ssl=False, skip_full_coverage_check=True,
                               password=args.auth)

for keyNameInt in range(args.startKey, args.startKey + args.num):

    keyName = args.keyPrefix + str(keyNameInt).zfill(args.keyNameLength)
    rc.delete(keyName)