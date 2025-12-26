from toolkit_helpers import logger


class SystemObj:
    def __init__(self, dlpx_obj, debug=None):
        self._dlpx = dlpx_obj
        self._debug = debug
        self._system = {}
        self._dns = None
        self._snmp = None
        self._snmpmanager = None
        self._time = None
        self._smtp = None
        self._syslog = None
        self._ldap = None
        self._sso = None
        self._storage = None
        self.LoadSystem()

    def LoadSystem(self):
        logger(self._debug, "Entering SystemObj.LoadSystem", 1)
        op = "resources/json/delphix/system"
        result, _fmt, _rc = self._dlpx.getJSONResult(op)
        if isinstance(result, dict) and result.get('status') == 'OK':
            self._system = result.get('result', {})
        else:
            print(f"No data returned for {op}. Try to increase timeout")

    def getStorage(self):
        logger(self._debug, "Entering SystemObj.getStorage", 1)
        total = self._system.get('storageTotal')
        used = self._system.get('storageUsed')
        if total is None or used is None:
            return {"Total": 'N/A', "Used": 'N/A', "Free": 'N/A', "pctused": 'N/A'}
        reserved = total * 0.1
        if reserved > 1024 * 1024 * 1024 * 1024:
            reserved = 1024 * 1024 * 1024 * 1024
        used_total = used + reserved
        free = total - used_total
        return {
            "Total": f"{total/1024/1024/1024:.2f}",
            "Used": f"{used_total/1024/1024/1024:.2f}",
            "Free": f"{free/1024/1024/1024:.2f}",
            "pctused": f"{used_total/total*100:.2f}",
        }

    def getVersion(self):
        ver = self._system.get('buildVersion') or {}
        return f"{ver.get('major','')}.{ver.get('minor','')}.{ver.get('micro','')}.{ver.get('patch','')}"

    def getUUID(self):
        return self._system.get('uuid')

    def getvCPU(self):
        count = 0
        for proc in self._system.get('processors', []) or []:
            try:
                count += int(proc.get('cores', 0))
            except Exception:
                pass
        return count

    def getvMem(self):
        mem = self._system.get('memorySize')
        return float(mem)/1024/1024/1024 if mem else 0

    def getEngineType(self):
        return self._system.get('engineType') or self._system.get('type') or 'N/A'

    def getDNSServers(self):
        if self._dns is None:
            op = "resources/json/delphix/service/dns"
            result, _fmt, _rc = self._dlpx.getJSONResult(op)
            if isinstance(result, dict) and result.get('status') == 'OK':
                self._dns = result.get('result', {})
            else:
                self._dns = {}
        return self._dns.get('servers', [])

    def getDNSDomains(self):
        if self._dns is None:
            self.getDNSServers()
        return self._dns.get('domain', []) if self._dns else []

    def getDNSSource(self):
        if self._dns is None:
            self.getDNSServers()
        if not self._dns:
            return 'STATIC'
        return self._dns.get('source', 'STATIC')

    def getSNMP(self):
        if self._snmp is None:
            op = "resources/json/delphix/service/snmp"
            result, _fmt, _rc = self._dlpx.getJSONResult(op)
            if isinstance(result, dict) and result.get('status') == 'OK':
                self._snmp = result.get('result', {})
            else:
                self._snmp = {}
        return self._snmp

    def getSNMPStatus(self):
        snmp = self.getSNMP()
        return "Enabled" if snmp.get('enabled') else "Disabled"

    def getSNMPSeverity(self):
        return self.getSNMP().get('severity')

    def getSNMPServers(self):
        if self._snmpmanager is None:
            op = "resources/json/delphix/service/snmp/manager"
            result, _fmt, _rc = self._dlpx.getJSONResult(op)
            if isinstance(result, dict) and result.get('status') == 'OK':
                self._snmpmanager = result.get('result', [])
            else:
                self._snmpmanager = []
        servers = []
        for item in self._snmpmanager:
            servers.append({
                'address': item.get('address'),
                'communityString': item.get('communityString')
            })
        return servers

    def getNTPServer(self):
        if self._time is None:
            op = "resources/json/delphix/service/time"
            result, _fmt, _rc = self._dlpx.getJSONResult(op)
            if isinstance(result, dict) and result.get('status') == 'OK':
                self._time = result.get('result', {})
            else:
                self._time = {}
        return self._time.get('ntpServers', []) or self._time.get('ntpServer', [])

    def getNTPStatus(self):
        if self._time is None:
            self.getNTPServer()
        return self._time.get('status', 'Disabled') if self._time else 'Disabled'

    def getSMTPServer(self):
        if self._smtp is None:
            op = "resources/json/delphix/service/smtp"
            result, _fmt, _rc = self._dlpx.getJSONResult(op)
            if isinstance(result, dict) and result.get('status') == 'OK':
                self._smtp = result.get('result', {})
            else:
                self._smtp = {}
        return self._smtp.get('server', 'N/A') if self._smtp else 'N/A'

    def getSMTPStatus(self):
        if self._smtp is None:
            self.getSMTPServer()
        if not self._smtp:
            return 'Disabled'
        return 'Enabled' if self._smtp.get('enabled') else 'Disabled'

    def getSyslog(self):
        if self._syslog is None:
            op = "resources/json/delphix/service/syslog"
            result, _fmt, _rc = self._dlpx.getJSONResult(op)
            if isinstance(result, dict) and result.get('status') == 'OK':
                self._syslog = result.get('result', {})
            else:
                self._syslog = {}
        return self._syslog

    def getSyslogStatus(self):
        syslog = self.getSyslog()
        return "Enabled" if syslog.get('enabled') else "Disabled"

    def getSyslogServers(self):
        syslog = self.getSyslog()
        return syslog.get('servers', []) if syslog else []

    def getSyslogSeverity(self):
        syslog = self.getSyslog()
        return syslog.get('severity') if syslog else None

    def getLDAP(self):
        if self._ldap is None:
            op = "resources/json/delphix/service/ldap"
            result, _fmt, _rc = self._dlpx.getJSONResult(op)
            if isinstance(result, dict) and result.get('status') == 'OK':
                self._ldap = result.get('result', {})
            else:
                self._ldap = {}
        return self._ldap

    def getLDAPStatus(self):
        ldap = self.getLDAP()
        return "Enabled" if ldap.get('enabled') else "Disabled"

    def getLDAPServers(self):
        ldap = self.getLDAP()
        return ldap.get('servers') if ldap else None

    def getSSO(self):
        if self._sso is None:
            op = "resources/json/delphix/service/sso"
            result, _fmt, _rc = self._dlpx.getJSONResult(op)
            if isinstance(result, dict) and result.get('status') == 'OK':
                self._sso = result.get('result', {})
            else:
                self._sso = {}
        return self._sso

    def getSSOStatus(self):
        sso = self.getSSO()
        return "Enabled" if sso.get('enabled') else "Disabled"

    def getSSOEntityId(self):
        return self.getSSO().get('entityId')

    def getSSOsamlMetadata(self):
        return self.getSSO().get('samlMetadata')

    def getSSOmaxAuthenticationAge(self):
        return self.getSSO().get('maxAuthenticationAge')

    def getSSOresponseSkewTime(self):
        return self.getSSO().get('responseSkewTime')

    def getStorageDevices(self):
        if self._storage is None:
            op = "resources/json/delphix/storage/device"
            result, _fmt, _rc = self._dlpx.getJSONResult(op)
            if isinstance(result, dict) and result.get('status') == 'OK':
                self._storage = result.get('result', [])
            else:
                self._storage = []
        return self._storage
