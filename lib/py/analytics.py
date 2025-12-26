"""Analytics manager - loads analytics list and exposes simple operations used by control script"""
from analytic_obj import AnalyticObj

# Optional specialized analytic implementations
try:
    from analytic_cpu_obj import AnalyticCPUObj
except Exception:
    AnalyticCPUObj = None
try:
    from analytic_network_obj import AnalyticNetworkObj
except Exception:
    AnalyticNetworkObj = None
try:
    from analytic_io_obj import AnalyticIOObj
except Exception:
    AnalyticIOObj = None
try:
    from analytic_nfs_obj import AnalyticNFSObj
except Exception:
    AnalyticNFSObj = None
try:
    from analytic_tcp_obj import AnalyticTCPObj
except Exception:
    AnalyticTCPObj = None

# Map statisticType to specialized class where available
_TYPE_MAP = {
    'CPU': AnalyticCPUObj,
    'CPU_UTIL': AnalyticCPUObj,
    'NETWORK': AnalyticNetworkObj,
    'NETWORK_UTIL': AnalyticNetworkObj,
    'NETWORK_INTERFACE_UTIL': AnalyticNetworkObj,  # Actual API value
    'IO': AnalyticIOObj,
    'IO_OPS': AnalyticIOObj,
    'DISK': AnalyticIOObj,
    'DISK_OPS': AnalyticIOObj,
    'ISCSI': AnalyticIOObj,
    'ISCSI_OPS': AnalyticIOObj,
    'iSCSI_OPS': AnalyticIOObj,  # Actual API value
    'NFS': AnalyticNFSObj or AnalyticIOObj,
    'NFS_OPS': AnalyticNFSObj or AnalyticIOObj,
    'TCP': AnalyticTCPObj,
    'TCP_OPS': AnalyticTCPObj,
    'TCP_CONNECTIONS': AnalyticTCPObj,
    'TCP_STATS': AnalyticTCPObj,  # Actual API value
}


class Analytics:
    def __init__(self, dlpx, debug=None):
        self._dlpx = dlpx
        self._debug = debug
        self._analytics = {}
        self.loadAnalyticsList()

    def loadAnalyticsList(self):
        op = 'resources/json/delphix/analytics'
        result, fmt, rc = self._dlpx.getJSONResult(op)
        if rc or result.get('status') != 'OK':
            print(f"No data returned for {op}. Try to increase timeout")
            return
        for stat in result.get('result', []):
            name = stat.get('name')
            ref = stat.get('reference')
            atype = stat.get('type')
            axes = stat.get('collectionAxes')
            interval = stat.get('collectionInterval')
            stype = stat.get('statisticType')
            cls = _TYPE_MAP.get(stype) or AnalyticObj
            ao = cls(self._dlpx, name, ref, atype, axes, interval, stype, self._debug)
            self._analytics[ref] = ao

    def getAnalyticsList(self):
        return sorted(list(self._analytics.keys()))

    def getAnalytics(self, ref):
        return self._analytics.get(ref)

    def getName(self, ref):
        obj = self.getAnalytics(ref)
        return obj.getName() if obj else None

    def getAnalyticByName(self, name):
        for ref, obj in self._analytics.items():
            if obj.getName() == name:
                return obj
        return None

    def create_analytic(self, name):
        # new analytic definitions supported: nfs-by-client, nfs-all, iscsi-by-client
        newanalytic = {}
        axes = ["latency", "throughput", "count", "op", "client"]
        newanalytic['nfs-by-client'] = {
            "type": "StatisticSlice",
            "name": "nfs-by-client",
            "collectionAxes": axes,
            "collectionInterval": 1,
            "statisticType": "NFS_OPS"
        }
        axes1 = axes + ["cached", "size"]
        newanalytic['nfs-all'] = {
            "type": "StatisticSlice",
            "name": "nfs-all",
            "collectionAxes": axes1,
            "collectionInterval": 1,
            "statisticType": "NFS_OPS"
        }
        newanalytic['iscsi-by-client'] = {
            "type": "StatisticSlice",
            "name": "iscsi-by-client",
            "collectionAxes": axes,
            "collectionInterval": 1,
            "statisticType": "iSCSI_OPS"
        }

        if name not in newanalytic:
            print(f"Invalid analytic name - {name}")
            return 1

        if self.getAnalyticByName(name):
            print(f"Analytic {name} already exists.")
            return 1

        operation = "resources/json/delphix/analytics"
        import json
        json_data = json.dumps(newanalytic[name])
        result, fmt, rc = self._dlpx.postJSONData(operation, json_data)
        if rc or result.get('status') != 'OK':
            print(f"Error: {result.get('error') if isinstance(result, dict) else result}")
            return 1
        else:
            print(f"New analytic {name} has been created")
            return 0


            return obj
        return None
