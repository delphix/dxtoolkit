from toolkit_helpers import logger


class NetworkObj:
    def __init__(self, dlpx_obj, debug=None):
        self._dlpx = dlpx_obj
        self._debug = debug
        self._network_latency = {}
        self._network_throughput = {}
        self._network_dsp = {}
        self.loadLatencyTest()
        self.loadThroughputTest()
        self.loadDSPTest()

    def _store_tests(self, target, records):
        for item in records:
            ref = item.get('reference')
            if ref:
                target[ref] = item

    def loadLatencyTest(self):
        logger(self._debug, "Entering NetworkObj.loadLatencyTest", 1)
        op = "resources/json/delphix/network/test/latency"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            self._store_tests(self._network_latency, result.get('result', []))
        else:
            print(f"No data returned for {op}. Try to increase timeout")

    def loadThroughputTest(self):
        logger(self._debug, "Entering NetworkObj.loadThroughputTest", 1)
        op = "resources/json/delphix/network/test/throughput"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            self._store_tests(self._network_throughput, result.get('result', []))
        else:
            print(f"No data returned for {op}. Try to increase timeout")

    def loadDSPTest(self):
        logger(self._debug, "Entering NetworkObj.loadDSPTest", 1)
        op = "resources/json/delphix/network/test/dsp"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            self._store_tests(self._network_dsp, result.get('result', []))
        else:
            print(f"No data returned for {op}. Try to increase timeout")

    def getHost(self, reference):
        if reference in self._network_latency:
            return self._network_latency[reference].get('parameters', {}).get('remoteHost')
        if reference in self._network_throughput:
            return self._network_throughput[reference].get('parameters', {}).get('remoteHost')
        if reference in self._network_dsp:
            return self._network_dsp[reference].get('parameters', {}).get('remoteHost')
        return None

    # --- Lightweight getters to match Perl API ---
    def getLatencyMax(self, reference):
        return (self._network_latency.get(reference) or {}).get('maximum', 'N/A')

    def getLatencyMin(self, reference):
        return (self._network_latency.get(reference) or {}).get('minimum', 'N/A')

    def getLatencyStdDev(self, reference):
        return (self._network_latency.get(reference) or {}).get('stddev', 'N/A')

    def getLatencyCount(self, reference):
        net = self._network_latency.get(reference) or {}
        return net.get('parameters', {}).get('requestCount', 'N/A')

    def getLatencySize(self, reference):
        net = self._network_latency.get(reference) or {}
        return net.get('parameters', {}).get('requestSize', 'N/A')

    def getLatencyLoss(self, reference):
        return (self._network_latency.get(reference) or {}).get('loss', 'N/A')

    def getLatencyAvg(self, reference):
        return (self._network_latency.get(reference) or {}).get('average', 'N/A')

    def getTestRate(self, reference):
        net = self._network_throughput.get(reference) or self._network_dsp.get(reference) or {}
        tp = net.get('throughput') or 0
        try:
            return f"{float(tp)/(1024*1024):.2f}"
        except Exception:
            return 'N/A'

    def getTestDirection(self, reference):
        net = self._network_throughput.get(reference) or self._network_dsp.get(reference) or {}
        return net.get('parameters', {}).get('direction', 'N/A')

    def getTestBlockSize(self, reference):
        net = self._network_throughput.get(reference) or self._network_dsp.get(reference) or {}
        return net.get('parameters', {}).get('blockSize', 'N/A')

    def getTestConnections(self, reference):
        net = self._network_throughput.get(reference) or self._network_dsp.get(reference) or {}
        return net.get('numConnections', 'N/A')

    def getState(self, reference):
        net = self._network_latency.get(reference) or self._network_throughput.get(reference) or self._network_dsp.get(reference) or {}
        return net.get('state', 'N/A')

    def getName(self, reference):
        net = self._network_latency.get(reference) or self._network_throughput.get(reference) or self._network_dsp.get(reference) or {}
        return net.get('name', 'N/A')

    def getLatencyTestsList(self, hostref=None):
        keys = list(self._network_latency.keys())
        if hostref:
            keys = [k for k in keys if (self._network_latency[k].get('parameters', {}).get('remoteHost') == hostref)]
        return sorted(keys)

    def getThroughputTestsList(self, hostref=None):
        keys = list(self._network_throughput.keys())
        if hostref:
            keys = [k for k in keys if (self._network_throughput[k].get('parameters', {}).get('remoteHost') == hostref)]
        return sorted(keys)

    def getDSPTestsList(self, hostref=None):
        keys = list(self._network_dsp.keys())
        if hostref:
            keys = [k for k in keys if (self._network_dsp[k].get('parameters', {}).get('remoteHost') == hostref)]
        return sorted(keys)

    def getLatencyLastTests(self, hostref):
        arr = self.getLatencyTestsList(hostref)
        return arr[-1:] if arr else []

    def getThroughputLastTests(self, hostref):
        arr = self.getThroughputTestsList(hostref)
        if not arr:
            return []
        tx = [ref for ref in arr if self.getTestDirection(ref) == 'TRANSMIT']
        rx = [ref for ref in arr if self.getTestDirection(ref) == 'RECEIVE']
        ret = []
        if tx:
            ret.append(tx[-1])
        if rx:
            ret.append(rx[-1])
        return ret

    def getDSPLastTests(self, hostref):
        arr = self.getDSPTestsList(hostref)
        if not arr:
            return []
        tx = [ref for ref in arr if self.getTestDirection(ref) == 'TRANSMIT']
        rx = [ref for ref in arr if self.getTestDirection(ref) == 'RECEIVE']
        ret = []
        if tx:
            ret.append(tx[-1])
        if rx:
            ret.append(rx[-1])
        return ret

    def runThroughputTest(self, hostref, direction=None, numconn=None, duration=None):
        logger(self._debug, "Entering NetworkObj.runThroughputTest", 1)
        op = "resources/json/delphix/network/test/throughput"
        payload = {
            "type": "NetworkThroughputTestParameters",
            "remoteHost": hostref,
            "duration": int(duration) if duration is not None else 60,
            "numConnections": int(numconn) if numconn is not None else 1,
            "direction": direction or 'TRANSMIT',
        }
        return self._run_job_operation(op, payload)

    def runLatencyTest(self, hostref, size=None, count=None):
        logger(self._debug, "Entering NetworkObj.runLatencyTest", 1)
        op = "resources/json/delphix/network/test/latency"
        payload = {
            "type": "NetworkLatencyTestParameters",
            "remoteHost": hostref,
            "requestSize": int(size) if size is not None else 8192,
            "requestCount": int(count) if count is not None else 60,
        }
        return self._run_job_operation(op, payload)

    def runDSPTest(self, hostref, direction=None, numconn=None, duration=None):
        logger(self._debug, "Entering NetworkObj.runDSPTest", 1)
        op = "resources/json/delphix/network/test/dsp"
        payload = {
            "type": "NetworkDSPTestParameters",
            "remoteHost": hostref,
            "direction": direction or 'TRANSMIT',
            "duration": int(duration) if duration is not None else 60,
            "numConnections": int(numconn) if numconn is not None else 1,
        }
        return self._run_job_operation(op, payload)

    def _run_job_operation(self, operation, payload):
        result, _fmt, _rc = self._dlpx.postJSONData(operation, payload)
        if isinstance(result, dict) and result.get('status') == 'OK':
            return result.get('job')
        if isinstance(result, dict) and result.get('error'):
            print(f"Problem with job {result.get('error', {}).get('details')}")
        else:
            print("Unknown error. Try with debug flag")
        return None
