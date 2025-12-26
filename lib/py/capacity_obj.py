from toolkit_helpers import logger


class CapacityObj:
    def __init__(self, dlpx_obj, debug=None):
        self._dlpx = dlpx_obj
        self._debug = debug
        self._databases = {}
        self._storagecontainers = []

    def LoadDatabases(self):
        logger(self._debug, "Entering CapacityObj.LoadDatabases", 1)
        op = "resources/json/delphix/capacity/consumer"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            for item in result.get('result', []):
                ref = item.get('container')
                if ref:
                    self._databases[ref] = item
                else:
                    sc = item.get('storageContainer')
                    if sc:
                        self._storagecontainers.append(sc)
                        self._databases[sc] = item
        else:
            print(f"No data returned for {op}. Try to increase timeout")

    def getStorageContainers(self):
        return self._storagecontainers

    def forcerefesh(self):
        op = "resources/json/delphix/capacity/refresh"
        result, _fmt, rc = self._dlpx.postJSONData(op, '{}')
        if rc or result.get('status') != 'OK':
            print("Problem with forcerefresh. Skipping results.")
            return 1
        jobno = result.get('job')
        if not jobno:
            return 1
        from toolkit_helpers import wait_for_job
        return wait_for_job(self._dlpx, jobno, "Capacity data refreshed.")

    def getDetailedDBUsage(self, db_ref, details=None):
        logger(self._debug, "Entering CapacityObj.getDetailedDBUsage", 1)
        db = self._databases.get(db_ref, {})
        breakdown = db.get('breakdown', {}) or {}
        def _gb(key):
            val = breakdown.get(key)
            return val/1024/1024/1024 if val is not None else 0
        dbutil = {
            'totalsize': _gb('actualSpace'),
            'currentcopy': _gb('activeSpace'),
            'dblogs': _gb('logSpace'),
            'snapshots_total': _gb('syncSpace'),
            'unvirtualized': _gb('unvirtualizedSpace'),
            'group_name': db.get('groupName'),
            'storageContainer': db.get('storageContainer'),
            'timestamp': db.get('timestamp'),
            'parent': db.get('parent'),
            'descendantSpace': _gb('descendantSpace'),
            'policySpace': _gb('policySpace'),
            'manualSpace': _gb('manualSpace'),
            'unownedSnapshotSpace': _gb('unownedSnapshotSpace'),
        }
        if details and str(details).lower() == 'all':
            # snapshots already include space in GB directly for >=1.9
            snaps_op = f"resources/json/delphix/capacity/snapshot?container={db_ref}"
            result, _fmt, _rc = self._dlpx.getJSONResult(snaps_op)
            snaps_list = []
            if isinstance(result, dict) and result.get('status') == 'OK':
                for snap in result.get('result', []):
                    snap['space'] = snap.get('space', 0)/1024/1024/1024 if snap.get('space') else 0
                    snaps_list.append(snap)
            sn_sum = sum([s.get('space', 0) for s in snaps_list])
            dbutil['snapshots_shared'] = dbutil['snapshots_total'] - sn_sum
            dbutil['snapshots_list'] = snaps_list
        return dbutil
