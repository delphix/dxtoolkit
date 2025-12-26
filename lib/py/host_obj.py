from toolkit_helpers import logger


class HostObj:
    def __init__(self, dlpx_obj, debug=None):
        self._dlpx = dlpx_obj
        self._debug = debug
        self._hosts = {}
        self.loadHostList()

    def loadHostList(self):
        logger(self._debug, "Entering HostObj.loadHostList", 1)
        op = "resources/json/delphix/host"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            for host in result.get('result', []):
                ref = host.get('reference')
                if ref:
                    self._hosts[ref] = host
        else:
            print(f"No data returned for {op}. Try to increase timeout")

    def getAllHosts(self):
        logger(self._debug, "Entering HostObj.getAllHosts", 1)
        return sorted(self._hosts.keys())

    def getHost(self, reference):
        logger(self._debug, "Entering HostObj.getHost", 1)
        return self._hosts.get(reference, {"name": "N/A"})

    def getHostByAddr(self, address):
        logger(self._debug, "Entering HostObj.getHostByAddr", 1)
        for ref, host in self._hosts.items():
            if host.get('name') == address:
                return ref
        return None

    def getHostAddr(self, reference):
        logger(self._debug, "Entering HostObj.getHostAddr", 1)
        host = self._hosts.get(reference)
        return host.get('name') if host else 'NA'
