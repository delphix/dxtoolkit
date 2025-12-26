"""Mock Engine for local runs and tests without a Delphix Engine.
Provides minimal responses for analytics, network tests, hosts, and databases.
"""
from datetime import datetime, timedelta, timezone


class MockEngine:
    def __init__(self, dever=None, debug=None):
        self._dever = dever
        self._debug = debug
        self._engine_name = 'MockEngine'
        self._ip = '127.0.0.1'

    # Methods used by scripts
    def load_config(self, config_file=None):
        return 0

    def dlpx_connect(self, engine_name):
        # Simulate successful connection
        self._engine_name = engine_name or self._engine_name
        return 0

    def getIP(self):
        return self._ip

    def getEngineName(self):
        return self._engine_name

    def getJSONResult(self, operation):
        # Return minimal payloads based on operation
        if operation == 'resources/json/delphix/analytics':
            return ({
                'status': 'OK',
                'result': [
                    {
                        'name': 'cpu',
                        'reference': 'analytic-cpu',
                        'type': 'StatisticSlice',
                        'collectionAxes': ['user', 'system', 'idle', 'iowait'],
                        'collectionInterval': 60,
                        'statisticType': 'CPU'
                    },
                    {
                        'name': 'network',
                        'reference': 'analytic-network',
                        'type': 'StatisticSlice',
                        'collectionAxes': ['bytes_in', 'bytes_out', 'packets_in', 'packets_out', 'errors'],
                        'collectionInterval': 60,
                        'statisticType': 'NETWORK'
                    },
                ]
            }, 'JSON', 0)
        if operation.startswith('resources/json/delphix/analytics/') and '/getData?' in operation:
            # Minimal datapoint streams
            now = datetime.now(timezone.utc)
            ts1 = (now - timedelta(minutes=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
            ts2 = (now - timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
            ref = operation.split('/')[4]
            if 'analytic-cpu' in ref:
                streams = [
                    {'client': 'none', 'op': None, 'datapoints': [
                        {'timestamp': ts1, 'user': 10, 'system': 5, 'idle': 80, 'iowait': 5},
                        {'timestamp': ts2, 'user': 20, 'system': 10, 'idle': 65, 'iowait': 5},
                    ]}
                ]
            else:
                streams = [
                    {'client': 'eth0', 'op': None, 'datapoints': [
                        {'timestamp': ts1, 'bytes_in': 1024*1024, 'bytes_out': 2*1024*1024, 'packets_in': 100, 'packets_out': 200, 'errors': 0},
                        {'timestamp': ts2, 'bytes_in': 3*1024*1024, 'bytes_out': 4*1024*1024, 'packets_in': 300, 'packets_out': 400, 'errors': 1},
                    ]}
                ]
            return ({'status': 'OK', 'result': {'datapointStreams': streams, 'overflow': False}}, 'JSON', 0)
        if operation == 'resources/json/delphix/network/test/latency':
            return ({'status': 'OK', 'result': [
                {'reference': 'lat-1', 'name': 'lat-test-1', 'state': 'COMPLETED',
                 'maximum': 500, 'minimum': 250, 'stddev': 30, 'average': 350, 'loss': 0,
                 'parameters': {'remoteHost': 'host-1', 'requestCount': 60, 'requestSize': 8192}}
            ]}, 'JSON', 0)
        if operation == 'resources/json/delphix/network/test/throughput':
            return ({'status': 'OK', 'result': [
                {'reference': 'thr-1', 'name': 'thr-test-1', 'state': 'COMPLETED', 'throughput': 10*1024*1024,
                 'parameters': {'remoteHost': 'host-1', 'direction': 'TRANSMIT', 'blockSize': 131072},
                 'numConnections': 8}
            ]}, 'JSON', 0)
        if operation == 'resources/json/delphix/host':
            return ({'status': 'OK', 'result': [
                {'reference': 'host-1', 'name': 'mock-host'}
            ]}, 'JSON', 0)
        if operation == 'resources/json/delphix/database':
            return ({'status': 'OK', 'result': [
                {'reference': 'db-1', 'name': 'mockdb', 'host': 'mock-host', 'provisionContainer': {'reference': 'prov-1'}}
            ]}, 'JSON', 0)
        # default empty
        return ({'status': 'OK', 'result': []}, 'JSON', 0)

    def postJSONData(self, operation, payload):
        # Simulate successful job submission
        if operation.startswith('resources/json/delphix/analytics/') and operation.endswith('/delete'):
            return ({'status': 'OK', 'job': 'job-1'}, 'JSON', 0)
        if operation.endswith('/pause') or operation.endswith('/resume'):
            return ({'status': 'OK', 'job': 'job-2'}, 'JSON', 0)
        if operation.endswith('/getData'):
            return ({'status': 'OK', 'job': 'job-3'}, 'JSON', 0)
        return ({'status': 'OK'}, 'JSON', 0)
