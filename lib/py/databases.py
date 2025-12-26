from toolkit_helpers import logger
from namespace_obj import NamespaceObj


class Databases:
    def __init__(self, dlpx_obj, debug=None):
        self._dlpx = dlpx_obj
        self._debug = debug
        self._dbs = {}
        self._namespace = NamespaceObj(dlpx_obj, debug)
        self.load()

    def load(self):
        logger(self._debug, "Entering Databases.load", 1)
        op = "resources/json/delphix/database"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            for db in result.get('result', []):
                ref = db.get('reference')
                if ref:
                    self._dbs[ref] = db
        else:
            print(f"No data returned for {op}. Try to increase timeout")

    def getDBByType(self, dbtype):
        # Match VDB vs dSource based on object type (case-insensitive)
        # VDB: OracleDatabaseContainer, MSSqlDatabaseContainer, etc. - must have provisionContainer
        # dSource: OracleDatabaseContainer, MSSqlDatabaseContainer, etc. - no provisionContainer
        matched = []
        search_lower = (dbtype or '').lower()
        for ref, db in self._dbs.items():
            has_provision = bool(db.get('provisionContainer'))
            # VDB has provisionContainer; dSource does not
            if search_lower == 'vdb' and has_provision:
                matched.append(ref)
            elif search_lower == 'dsource' and not has_provision:
                matched.append(ref)
        return matched

    def getDBList(self):
        return list(self._dbs.keys())

    def getDB(self, ref):
        return self._dbs.get(ref)

    def getDBByName(self, name):
        return [ref for ref, db in self._dbs.items() if db.get('name') == name]

    def getDBForGroup(self, group_ref):
        return [ref for ref, db in self._dbs.items() if db.get('group') == group_ref]

    def getDBForHost(self, host, instance=None):
        return [ref for ref, db in self._dbs.items() if db.get('host') == host]

    def getDBByParent(self, parent):
        return [ref for ref, db in self._dbs.items() if db.get('parent') == parent]

    def isReplica(self, ref):
        db = self._dbs.get(ref)
        if not db:
            return False
        return bool(db.get('namespace'))

    def getName(self, ref):
        db = self._dbs.get(ref)
        if not db:
            return None
        name = db.get('name')
        nsref = db.get('namespace')
        if nsref:
            ns_name = self._namespace.getName(nsref)
            if ns_name:
                return f"{name}@{ns_name}"
        return name

    def getEnvironmentName(self, ref):
        db = self._dbs.get(ref)
        return db.get('environmentName') if db else None

    def getParentName(self, ref):
        db = self._dbs.get(ref)
        return db.get('parentName') if db else None
