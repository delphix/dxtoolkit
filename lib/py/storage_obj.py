from toolkit_helpers import logger, convert_from_utc


class StorageObj:
    def __init__(self, dlpx_obj, debug=None):
        self._dlpx = dlpx_obj
        self._debug = debug
        self._storage_test = {}
        self._devices = {}

    def LoadStorageTest(self):
        logger(self._debug, "Entering StorageObj.LoadStorageTest", 1)
        op = "resources/json/delphix/storage/test"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            for item in result.get('result', []):
                ref = item.get('reference')
                if ref:
                    self._storage_test[ref] = item
        else:
            print(f"No data returned for {op}. Try to increase timeout")

    def getTestList(self):
        return sorted(self._storage_test.keys())

    def isTestExist(self, ref):
        return ref in self._storage_test

    def getState(self, ref):
        item = self._storage_test.get(ref)
        return item.get('state') if item else None

    def getStartTime(self, ref):
        item = self._storage_test.get(ref)
        if not item:
            return None
        tz = self._dlpx.getTimezone()
        return convert_from_utc(item.get('startTime'), tz, drop_fraction=1)

    def parseTestResults(self, ref):
        item = self._storage_test.get(ref)
        if not item or not item.get('testResults'):
            return
        mapping = {}
        for t in item.get('testResults', []):
            name = t.get('testName')
            if name:
                mapping[name] = t
        item['_test_results_hash'] = mapping

    def _get_result(self, ref, testname):
        item = self._storage_test.get(ref)
        if not item:
            return None
        if '_test_results_hash' not in item:
            self.parseTestResults(ref)
        return item.get('_test_results_hash', {}).get(testname)

    def getLatencyGrade(self, ref, testname):
        r = self._get_result(ref, testname)
        return r.get('latencyGrade') if r else None

    def getLatencyMin(self, ref, testname):
        r = self._get_result(ref, testname)
        return r.get('minLatency') if r else None

    def getLatencyMax(self, ref, testname):
        r = self._get_result(ref, testname)
        return r.get('maxLatency') if r else None

    def getLatencyAvg(self, ref, testname):
        r = self._get_result(ref, testname)
        return r.get('averageLatency') if r else None

    def getLatency95(self, ref, testname):
        r = self._get_result(ref, testname)
        return r.get('latency95thPercentile') if r else None

    def getLatencyStdDev(self, ref, testname):
        r = self._get_result(ref, testname)
        return r.get('stddevLatency') if r else None

    def getTestIOPS(self, ref, testname):
        r = self._get_result(ref, testname)
        return r.get('iops') if r else None

    def getTestThoughput(self, ref, testname):
        r = self._get_result(ref, testname)
        return f"{r.get('throughput')/1024/1024:10.2f}" if r and r.get('throughput') is not None else None

    def generateIORC(self, ref, filename):
        op = f"resources/json/delphix/storage/test/{ref}/result"
        result, _fmt, rc = self._dlpx.postJSONData(op, '{}')
        if not rc and isinstance(result, dict) and result.get('status') == 'OK':
            text = result.get('result')
            try:
                with open(filename, 'w') as fd:
                    fd.write(text)
                return 0
            except Exception:
                print(f"Can't open file {filename}")
                return 1
        print(f"No data returned for {op}. Try to increase timeout")
        return 1

    def getDisks(self, _dummy):
        if not self._devices:
            op = "resources/json/delphix/storage/device"
            result, _fmt, _rc = self._dlpx.getJSONResult(op)
            if isinstance(result, dict) and result.get('status') == 'OK':
                for dev in result.get('result', []):
                    ref = dev.get('reference') or dev.get('name')
                    if ref:
                        self._devices[ref] = dev
        return list(self._devices.values())
