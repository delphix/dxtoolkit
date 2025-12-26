from toolkit_helpers import logger
from namespace_obj import NamespaceObj


class GroupObj:
    def __init__(self, dlpx_obj, debug=None):
        self._dlpx = dlpx_obj
        self._debug = debug
        self._groups = {}
        self._namespace = NamespaceObj(dlpx_obj, debug)
        self.load()

    def load(self):
        logger(self._debug, "Entering GroupObj.load", 1)
        op = "resources/json/delphix/group"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            for grp in result.get('result', []):
                ref = grp.get('reference')
                if ref:
                    self._groups[ref] = grp
        else:
            print(f"No data returned for {op}. Try to increase timeout")

    def getName(self, ref):
        grp = self._groups.get(ref)
        if not grp:
            return None
        name = grp.get('name')
        nsref = grp.get('namespace')
        if nsref:
            ns_name = self._namespace.getName(nsref)
            if ns_name:
                return f"{name}@{ns_name}"
        return name

    def getGroupByName(self, name):
        for ref, grp in self._groups.items():
            if grp.get('name') == name:
                return grp
        return None
