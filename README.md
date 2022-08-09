# pyredis_tdemo_utils
Utilities for benchmarking pyredis

Uses a python metaclass and decorators to add metric and retry logic to existing code.

Very experimental and not definitely not suitable for production use.

Look at simple-set-get.py for easy example, zset-and-hash.py is much more involved, including aggregating and reporting metrics from multiple workers.

Requires pyRedis 3, not compatible with pyRedis 4

pip3 install redis==3.5.3
pip3 install pyyaml