import time
from toolkit_helpers import logger


class JobsObj:
    """Lightweight job wrapper to poll Delphix jobs."""

    def __init__(self, dlpx_obj, job=None, silent=None, debug=None):
        self._dlpx = dlpx_obj
        self._job = job
        self._silent = silent
        self._debug = debug
        self._joboutput = None
        if job:
            self.loadJob()

    def loadJob(self):
        logger(self._debug, "Entering JobsObj.loadJob", 1)
        op = f"resources/json/delphix/job/{self._job}"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            self._joboutput = result.get('result')
        else:
            logger(self._debug, f"No data for {op}", 1)

    def getJobState(self):
        return (self._joboutput or {}).get('jobState')

    def getPercentage(self):
        return (self._joboutput or {}).get('percentComplete')

    def getLastMessage(self):
        events = (self._joboutput or {}).get('events') or []
        return (events[-1] or {}).get('messageDetails') if events else None

    def waitForJob(self):
        logger(self._debug, "Entering JobsObj.waitForJob", 1)
        oldpct = self.getPercentage()
        if self._silent == 'true' and oldpct is not None:
            try:
                print(f"{oldpct}", end='', flush=True)
            except Exception:
                print(f"{oldpct}")
        while True:
            state = self.getJobState()
            if state in ('COMPLETED', 'CANCELED', 'FAILED'):
                break
            time.sleep(1)
            self.loadJob()
            if self._silent == 'true':
                newpct = self.getPercentage()
                if newpct is not None and newpct != oldpct:
                    print(f" - {newpct}", end='', flush=True)
                    oldpct = newpct
        if self._silent == 'true':
            print("\nJob {} finished with state: {}".format(self._job, state))
            if state != 'COMPLETED':
                msg = self.getLastMessage()
                if msg:
                    print(f"Last message is: {msg}")
        return state
