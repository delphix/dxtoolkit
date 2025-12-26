"""Minimal Environment object port used by Analytic_tcp_obj"""
from toolkit_helpers import logger


class EnvironmentObj:
    def __init__(self, dlpx, debug=None):
        self._dlpx = dlpx
        self._debug = debug
        self._envlisteners = {}
        # try to populate listeners immediately
        self.getEnvironmentListeners()

    def getAllEnvironmentListenersPorts(self):
        logger(self._debug, "Entering EnvironmentObj::getAllEnvironmentListenersPorts", 1)
        ports = []
        for env in sorted(self._envlisteners.keys()):
            vals = self.getEnvironmentListenerPorts(env)
            if isinstance(vals, list):
                ports.extend(vals)
        return {p: 1 for p in ports}

    def getEnvironmentListenerPorts(self, env):
        # return list of listener ports for a single environment
        vals = []
        for ref, item in (self._envlisteners.get(env, {}) or {}).items():
            if 'port' in item:
                vals.append(str(item['port']))
        return vals

    def getEnvironmentListeners(self):
        logger(self._debug, "Entering EnvironmentObj::getEnvironmentListeners", 1)
        op = "resources/json/delphix/environment/oracle/listener"
        result, fmt, rc = self._dlpx.getJSONResult(op)
        if rc or result.get('status') != 'OK':
            return
        for envlist in result.get('result', []):
            env = envlist.get('environment')
            ref = envlist.get('reference')
            if env not in self._envlisteners:
                self._envlisteners[env] = {}
            self._envlisteners[env][ref] = envlist
