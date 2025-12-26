from toolkit_helpers import logger


class NamespaceObj:
    def __init__(self, dlpx_obj, debug=None):
        self._dlpx = dlpx_obj
        self._debug = debug
        self._namespaces = {}
        self.load()

    def load(self):
        logger(self._debug, "Entering NamespaceObj.load", 1)
        op = "resources/json/delphix/namespace"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            for ns in result.get('result', []):
                ref = ns.get('reference')
                if ref:
                    self._namespaces[ref] = ns
        else:
            print(f"No data returned for {op}. Try to increase timeout")

    def getName(self, ref):
        ns = self._namespaces.get(ref)
        return ns.get('name') if ns else None
