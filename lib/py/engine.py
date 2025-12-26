"""Minimal Engine port providing enough API used by analytics control script.
This is a pragmatic port: it implements sessionless GET/POST calls with requests
and basic config loading. It does not implement full auth flows from Perl.
"""
import json
import os
import tempfile
import getpass
import stat
import subprocess
import platform
import socket
import requests
from requests.exceptions import RequestException
import urllib3
import http.cookiejar as cookiejar
from hashlib import md5
try:
    from Crypto.Cipher import Blowfish
except Exception:
    Blowfish = None


class Engine:
    def __init__(self, dever=None, debug=None):
        self._debug = debug
        self._dever = dever
        self._engines = {}
        self._session = requests.Session()
        self._session.verify = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self._api = None

    def _print_http(self, direction, method, url, req_headers=None, req_body=None, status=None, resp_headers=None, resp_body=None):
        # Print minimal HTTP request/response info when debug >= 2
        try:
            dbg = int(self._debug) if self._debug is not None else 0
        except Exception:
            dbg = 0
        if dbg < 2:
            return
        print(f"[HTTP] {direction} {method} {url}")
        if req_headers:
            print(f"[HTTP] Request headers: {req_headers}")
        if req_body:
            try:
                print(f"[HTTP] Request body: {req_body}"[:2000])
            except Exception:
                pass
        if status is not None:
            print(f"[HTTP] Response status: {status}")
        if resp_headers:
            print(f"[HTTP] Response headers: {resp_headers}")
        if resp_body:
            try:
                # avoid printing extremely large bodies
                print(f"[HTTP] Response body: {resp_body}"[:4000])
            except Exception:
                pass

    def _base_url(self):
        return f"{self._protocol}://{self._host}:{self._port}"

    def load_config(self, fn=None):
        if fn:
            cfg = fn
        elif 'DXTOOLKIT_CONF' in os.environ:
            cfg = os.environ['DXTOOLKIT_CONF']
        else:
            here = os.path.dirname(os.path.dirname(__file__))
            cfg = os.path.join(here, 'dxtools.conf')
        with open(cfg) as fh:
            data = json.load(fh)
        engines = {}
        for host in data.get('data', []):
            name = host.get('hostname')
            if not name:
                continue
            entry = {}
            # copy known fields with sensible defaults
            entry['ip_address'] = host.get('ip_address')
            # coerce numeric fields
            try:
                entry['port'] = int(host.get('port', 80))
            except Exception:
                entry['port'] = 80
            entry['protocol'] = host.get('protocol', 'http')
            entry['default'] = host.get('default', 'false')
            try:
                entry['timeout'] = int(host.get('timeout', 60))
            except Exception:
                entry['timeout'] = 60
            entry['username'] = host.get('username')
            entry['password'] = host.get('password')
            entry['clientid'] = host.get('clientid')
            entry['clientsecret'] = host.get('clientsecret')
            entry['oauth2_token'] = host.get('oauth2_token')
            entry['oauth2_token_var'] = host.get('oauth2_token_var')
            entry['encrypted'] = host.get('encrypted', 'false')
            entry['passwordvar'] = host.get('passwordvar')
            entry['passwordscript'] = host.get('passwordscript')
            entry['additionalopt'] = host.get('additionalopt')
            entry['prevalidate'] = host.get('prevalidate', 'false')
            engines[name] = entry
        self._engines = engines

    def getAllEngines(self):
        return sorted(list(self._engines.keys()))

    def getDefaultEngines(self):
        return [k for k, v in sorted(self._engines.items()) if v.get('default') == 'true']

    def getEngine(self, name):
        return self._engines.get(name)

    def getIP(self):
        return getattr(self, '_host', None)

    def getEngineName(self):
        return getattr(self, '_enginename', 'unknown')

    def getTimezone(self):
        # Return engine timezone by querying service/time
        try:
            op = 'resources/json/delphix/service/time'
            result, fmt, rc = self.getJSONResult(op)
            if rc or result.get('status') != 'OK':
                return None
            return result.get('result', {}).get('systemTimeZone')
        except Exception:
            return None

    def getTime(self, minus=None):
        """Return engine current time as ISO Zulu timestamp (UTC).

        If `minus` is provided (minutes), return time minus that many minutes.
        This mirrors the Perl Engine::getTime used by Toolkit_helpers->timestamp.
        """
        try:
            op = 'resources/json/delphix/service/time'
            result, fmt, rc = self.getJSONResult(op)
            if rc or result.get('status') != 'OK':
                return None
            cur = result.get('result', {}).get('currentTime')
            if not cur:
                return None
            # parse ISO timestamps like 2025-12-19T12:34:56.000Z
            from datetime import datetime, timedelta, timezone
            try:
                # Python 3.11+: fromisoformat doesn't accept 'Z', replace with +00:00
                if cur.endswith('Z'):
                    cur2 = cur[:-1] + '+00:00'
                else:
                    cur2 = cur
                dt = datetime.fromisoformat(cur2)
            except Exception:
                # fallback to common parsing
                try:
                    dt = datetime.strptime(cur, '%Y-%m-%dT%H:%M:%S.%fZ')
                    dt = dt.replace(tzinfo=timezone.utc)
                except Exception:
                    try:
                        dt = datetime.strptime(cur, '%Y-%m-%dT%H:%M:%SZ')
                        dt = dt.replace(tzinfo=timezone.utc)
                    except Exception:
                        return None

            if minus is not None:
                try:
                    m = int(minus)
                    dt = dt - timedelta(minutes=m)
                except Exception:
                    pass

            # return Zulu format without fractional seconds
            return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception:
            return None

    def getApi(self):
        return getattr(self, '_api', None)

    def getCurrentUserType(self):
        try:
            op = 'resources/json/delphix/user/current'
            result, _fmt, rc = self.getJSONResult(op)
            if rc or result.get('status') != 'OK':
                return None
            return result.get('result', {}).get('userType')
        except Exception:
            return None

    def _configure_verify(self, cfg):
        """Configure TLS verification from config or env.

        - If DXTOOLKIT_VERIFY env or cfg['verify'] is 'true'/'1'/'yes', enable verification.
        - If it points to a file path, use it as CA bundle.
        - If 'false' or unset, keep verify disabled and suppress warnings.
        """
        val = cfg.get('verify') or os.environ.get('DXTOOLKIT_VERIFY')
        if not val:
            self._session.verify = False
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            return

        sval = str(val).lower()
        if sval in ('true', '1', 'yes'):
            self._session.verify = True
            return
        if sval in ('false', '0', 'no'):
            self._session.verify = False
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            return
        # otherwise treat as path to CA bundle
        self._session.verify = val

    def dlpx_connect(self, engine, silent=False):
        cfg = self._engines.get(engine)
        if not cfg:
            print(f"Can't find {engine} in config file.")
            return 1
        self._host = cfg.get('ip_address')
        self._port = cfg.get('port')
        self._protocol = cfg.get('protocol')
        self._enginename = engine
        self._configure_verify(cfg)
        # if encrypted credentials present, attempt to decrypt them
        if cfg.get('encrypted') == 'true':
            # decrypt password and/or clientsecret if present
            try:
                self._try_decrypt_engine(cfg)
            except Exception:
                # continue even if decryption fails; higher-level code will error
                pass
        # determine login type (username, clientid, oauth)
        if cfg.get('username'):
            login_type = 'password'
        elif cfg.get('clientid'):
            login_type = 'apikeys'
        elif cfg.get('oauth2_token') or cfg.get('oauth2_token_var'):
            login_type = 'oauth'
        else:
            print("Username, clientid or oauth2_token are missing from config file")
            return 1

        # create cookie file per engine, mirror Perl behavior
        cookie_dir = tempfile.gettempdir()
        cookie_file = os.path.join(cookie_dir, f"cookies.{getpass.getuser()}.{engine}")
        try:
            cj = cookiejar.LWPCookieJar(cookie_file)
            # load existing cookies if present
            try:
                cj.load(ignore_discard=True, ignore_expires=True)
            except Exception:
                # no cookie file yet
                pass
            self._session.cookies = cj
            # persist immediately
            try:
                cj.save(ignore_discard=True, ignore_expires=True)
            except Exception:
                pass
            # try to secure cookie file on non-windows
            if platform.system() != 'Windows':
                try:
                    os.chmod(cookie_file, stat.S_IRUSR | stat.S_IWUSR)
                except Exception:
                    pass
        except Exception:
            # fallback: leave session cookies in-memory
            pass
        base = self._base_url()
        # determine API version
        try:
            url_about = f"{base}/resources/json/delphix/about"
            self._print_http('=>', 'GET', url_about)
            r = self._session.get(url_about, timeout=cfg.get('timeout', 60))
            # print response
            try:
                txt = r.text
            except Exception:
                txt = None
            self._print_http('<=', 'GET', url_about, status=getattr(r, 'status_code', None), resp_headers=getattr(r, 'headers', None), resp_body=txt)
            r.raise_for_status()
            js = r.json()
            if 'result' in js and 'apiVersion' in js['result']:
                api = js['result']['apiVersion']
                ses_version = f"{api.get('major')}.{api.get('minor')}.{api.get('micro')}"
                self._api = ses_version
            else:
                ses_version = None
        except RequestException as e:
            if not silent:
                print(f"Can't connect to Dephix Engine {engine}: {e}")
            return 1

        # prevalidate: extended password support if requested
        if cfg.get('prevalidate') == 'true':
            if self.extended_password(cfg):
                print("Error with extended password support. Skipping engine")
                return 1

        # token-based login if provided
        token = None
        token_type = None
        if cfg.get('oauth2_token'):
            token = cfg.get('oauth2_token')
            token_type = 'oauth'
        elif cfg.get('oauth2_token_var'):
            token = os.environ.get(cfg.get('oauth2_token_var'))
            token_type = 'oauth'
        elif cfg.get('clientid') and cfg.get('clientsecret'):
            # obtain SSO token via client credentials
            sso_provider = cfg.get('sso_provider')
            sso_scope = cfg.get('sso_scope', 'groups')
            token = self.getSSOToken(cfg.get('clientid'), cfg.get('clientsecret'), sso_provider=sso_provider, scope=sso_scope)
            token_type = 'apikeys'

        if token:
            ok = self.token_login(token_type, token, ses_version or self._api)
            if ok:
                if not silent:
                    print(f"token login to {engine} failed.")
                return 1
            # persist cookies if we have a file-backed cookiejar
            try:
                cj = self._session.cookies
                if hasattr(cj, 'save'):
                    cj.save(ignore_discard=True, ignore_expires=True)
            except Exception:
                pass
            return 0

        # fallback to username/password session login
        if cfg.get('username'):
            self._user = cfg.get('username')
            self._password = cfg.get('password') or ''
            # create session for version
            if ses_version:
                if self.session(ses_version):
                    if not silent:
                        print(f"session authentication to {engine} failed.")
                    return 1
            else:
                # try default session
                if self.session('1.3.0'):
                    if not silent:
                        print(f"session authentication to {engine} failed.")
                    return 1

            if self._password == '':
                # try read from environment variable if configured
                if cfg.get('passwordvar'):
                    self._password = os.environ.get(cfg.get('passwordvar'), '')

            if self.login():
                if not silent:
                    print(f"login to {engine} failed.")
                return 1
            return 0

        # nothing to login with
        return 0

    def extended_password(self, engine_config):
        # mirror Perl extended_password: try passwordvar then passwordscript
        if engine_config.get('passwordvar') and engine_config.get('passwordvar') != '':
            var = engine_config.get('passwordvar')
            val = os.environ.get(var)
            if val is None:
                return 1
            self._password = val
            return 0
        elif engine_config.get('passwordscript') and engine_config.get('passwordscript') != '':
            script = engine_config.get('passwordscript')
            # build command: script enginename username ip [additionalopt]
            cmd = [script, self._enginename, engine_config.get('username',''), engine_config.get('ip_address','')]
            if engine_config.get('additionalopt'):
                # split additionalopt into multiple args (whitespace separated)
                extra = engine_config.get('additionalopt')
                cmd.extend(extra.split())
            try:
                out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True)
                self._password = out.strip()
                return 0
            except Exception:
                return 1
        else:
            return 1

    def _read_delkey(self):
        # Try environment first
        env = os.environ.get('DX_ENCKEY')
        if env:
            return env
        # Try to parse lib/dbutils.pm
        try:
            here = os.path.dirname(os.path.dirname(__file__))
            path = os.path.join(here, 'dbutils.pm')
            if not os.path.exists(path):
                return None
            with open(path) as fh:
                for line in fh:
                    line = line.strip()
                    if line.startswith('our $delkey') or line.startswith('my $delkey') or 'our $delkey' in line:
                        # expect: our $delkey = "...";
                        parts = line.split('=')
                        if len(parts) >= 2:
                            rhs = parts[1].strip().rstrip(';')
                            # remove quotes
                            if rhs.startswith('"') and rhs.endswith('"'):
                                return rhs.strip('"')
                            if rhs.startswith("'") and rhs.endswith("'"):
                                return rhs.strip("'")
        except Exception:
            return None
        return None

    def _try_decrypt_engine(self, cfg):
        # Attempt to decrypt password and clientsecret if present and encrypted
        if Blowfish is None:
            return
        delkey = self._read_delkey()
        if not delkey:
            return
        host = cfg.get('ip_address', '')
        # hostname for key derivation
        hostname = socket.gethostname()

        def _decrypt_value(encval, keypart, with_host=True):
            if not encval:
                return None
            # build key
            if with_host:
                keystr = f"{host}{delkey}{keypart}{hostname}"
            else:
                keystr = f"{host}{delkey}{keypart}"
            keyb = keystr.encode('utf-8')
            iv = (host + keypart)[:8].encode('utf-8')
            try:
                cipher = Blowfish.new(keyb, Blowfish.MODE_CBC, iv)
                data = bytes.fromhex(encval)
                dec = cipher.decrypt(data)
                try:
                    s = dec.decode('utf-8')
                except Exception:
                    s = dec.decode('latin-1')
                if len(s) >= 32:
                    plain = s[:-32]
                    checksum = s[-32:]
                    if md5(plain.encode('utf-8')).hexdigest() == checksum:
                        return plain
            except Exception:
                pass
            return None

        # password
        if cfg.get('password'):
            enc = cfg.get('password')
            # try with host
            val = _decrypt_value(enc, cfg.get('username',''), with_host=True)
            if val is None:
                val = _decrypt_value(enc, cfg.get('username',''), with_host=False)
            if val is None:
                # old method: strip first char and try without checksum
                try:
                    keystr = f"{host}{delkey}{cfg.get('username','')}"
                    keyb = keystr.encode('utf-8')
                    iv = (host + cfg.get('username',''))[:8].encode('utf-8')
                    cipher = Blowfish.new(keyb, Blowfish.MODE_CBC, iv)
                    trimmed = enc[1:]
                    dec = cipher.decrypt(bytes.fromhex(trimmed))
                    try:
                        val = dec.decode('utf-8')
                    except Exception:
                        val = dec.decode('latin-1')
                except Exception:
                    val = None
            if val:
                cfg['password'] = val

        # clientsecret
        if cfg.get('clientsecret'):
            enc = cfg.get('clientsecret')
            val = _decrypt_value(enc, cfg.get('clientid',''), with_host=True)
            if val is None:
                val = _decrypt_value(enc, cfg.get('clientid',''), with_host=False)
            if val is None:
                try:
                    keystr = f"{host}{delkey}{cfg.get('clientid','')}"
                    keyb = keystr.encode('utf-8')
                    iv = (host + cfg.get('clientid',''))[:8].encode('utf-8')
                    cipher = Blowfish.new(keyb, Blowfish.MODE_CBC, iv)
                    trimmed = enc[1:]
                    dec = cipher.decrypt(bytes.fromhex(trimmed))
                    try:
                        val = dec.decode('utf-8')
                    except Exception:
                        val = dec.decode('latin-1')
                except Exception:
                    val = None
            if val:
                cfg['clientsecret'] = val

    def getJSONResult(self, operation):
        base = self._base_url()
        url = f"{base}/{operation}"
        try:
            self._print_http('=>', 'GET', url)
            r = self._session.get(url, timeout=self._engines[self._enginename].get('timeout', 60))
            try:
                resp_text = r.text
            except Exception:
                resp_text = None
            self._print_http('<=', 'GET', url, status=getattr(r, 'status_code', None), resp_headers=getattr(r, 'headers', None), resp_body=resp_text)
            r.raise_for_status()
            js = r.json()
            return js, json.dumps(js, indent=2), 0
        except RequestException as e:
            self._print_http('<=', 'GET', url, status=None, resp_body=str(e))
            return {'status': 'ERROR', 'error': str(e)}, str(e), 1

    def postJSONData(self, operation, post_data):
        base = self._base_url()
        url = f"{base}/{operation}"
        headers = {'Content-Type': 'application/json'}
        try:
            # allow passing either JSON string or Python dict
            self._print_http('=>', 'POST', url, req_headers=headers, req_body=post_data)
            if isinstance(post_data, (dict, list)):
                r = self._session.post(url, json=post_data, headers=headers, timeout=self._engines[self._enginename].get('timeout', 60))
            else:
                r = self._session.post(url, data=post_data, headers=headers, timeout=self._engines[self._enginename].get('timeout', 60))
            try:
                resp_text = r.text
            except Exception:
                resp_text = None
            self._print_http('<=', 'POST', url, status=getattr(r, 'status_code', None), resp_headers=getattr(r, 'headers', None), resp_body=resp_text)
            r.raise_for_status()
            js = r.json()
            return js, json.dumps(js, indent=2), 0
        except RequestException as e:
            self._print_http('<=', 'POST', url, status=None, resp_body=str(e))
            return {'status': 'ERROR', 'error': str(e)}, str(e), 1

    def session(self, version=None):
        # create API session with Delphix engine
        if version:
            try:
                major, minor, micro = version.split('.')
            except Exception:
                # Match Perl's default API version (1.3.0)
                major, minor, micro = '1', '3', '0'
        else:
            # Match Perl's default API version (1.3.0)
            major, minor, micro = '1', '3', '0'

        mysession = {
            "type": "APISession",
            "version": {
                "type": "APIVersion",
                "major": int(major),
                "minor": int(minor),
                "micro": int(micro)
            }
        }

        operation = "resources/json/delphix/session"
        js, fmt, rc = self.postJSONData(operation, mysession)
        if rc or (isinstance(js, dict) and js.get('status') == 'ERROR'):
            return 1
        return 0

    def getSession(self):
        operation = "resources/json/delphix/session"
        js, fmt, rc = self.getJSONResult(operation)
        if rc or (isinstance(js, dict) and js.get('status') == 'ERROR'):
            return 1, None
        ver = js.get('result', {}).get('version')
        if ver:
            ver_api = f"{ver.get('major')}.{ver.get('minor')}.{ver.get('micro')}"
        else:
            ver_api = None
        return 0, ver_api

    def login(self):
        # credentials must be set in self._user and self._password
        if not getattr(self, '_user', None):
            return 1
        mylogin = {
            "type": "LoginRequest",
            "username": self._user,
            "password": self._password
        }
        operation = "resources/json/delphix/login"
        js, fmt, rc = self.postJSONData(operation, mylogin)
        if rc or (isinstance(js, dict) and js.get('status') == 'ERROR'):
            return 1
        # set current user if returned
        try:
            if js.get('result') and js['result'].get('name'):
                self._currentuser = js['result'].get('name')
        except Exception:
            pass
        return 0

    def token_login(self, login_type, token, version=None):
        # login using bearer token; login_type may be 'apikeys' or 'oauth'
        if not token:
            return 1
        if login_type == 'apikeys':
            operation = "sso/virtualization/api/login"
        else:
            operation = "virtualization/api/oauth2-login"

        mysession = {
            "type": "APISession",
            "version": {
                "type": "APIVersion",
                "major": int(version.split('.')[0]) if version else 1,
                "minor": int(version.split('.')[1]) if version and len(version.split('.'))>1 else 3,
                "micro": int(version.split('.')[2]) if version and len(version.split('.'))>2 else 0
            }
        }

        url = f"{self._protocol}://{self._host}:{self._port}/{operation}"
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
        try:
            self._print_http('=>', 'POST', url, req_headers=headers, req_body=mysession)
            r = self._session.post(url, json=mysession, headers=headers, timeout=self._engines[self._enginename].get('timeout', 60))
            try:
                resp_txt = r.text
            except Exception:
                resp_txt = None
            self._print_http('<=', 'POST', url, status=getattr(r, 'status_code', None), resp_headers=getattr(r, 'headers', None), resp_body=resp_txt)
            r.raise_for_status()
            # try to inspect response for errors
            try:
                js = r.json()
                if isinstance(js, dict) and js.get('status') == 'ERROR':
                    # clear cookies on auth error
                    self._session.cookies.clear()
                    return 1
            except Exception:
                pass
            # persist cookies if file-backed
            try:
                cj = self._session.cookies
                if hasattr(cj, 'save'):
                    cj.save(ignore_discard=True, ignore_expires=True)
            except Exception:
                pass
            return 0
        except RequestException as e:
            self._print_http('<=', 'POST', url, status=None, resp_body=str(e))
            # clear cookies on auth error
            try:
                self._session.cookies.clear()
            except Exception:
                pass
            return 1

    def logout(self):
        operation = "resources/json/delphix/logout"
        js, fmt, rc = self.postJSONData(operation, {})
        self._session.cookies.clear()
        if rc or (isinstance(js, dict) and js.get('status') == 'ERROR'):
            return 1
        return 0

    def getOauth_token(self, engine_config):
        # mirror Perl getOauth_token: prefer env var when oauth2_token_var is set
        if engine_config.get('oauth2_token_var') and engine_config.get('oauth2_token'):
            # mutually exclusive
            return None
        if engine_config.get('oauth2_token_var'):
            var = engine_config.get('oauth2_token_var')
            val = os.environ.get(var)
            return val
        if engine_config.get('oauth2_token'):
            return engine_config.get('oauth2_token')
        return None

    def getSSOToken(self, client_id, client_secret):
        # Perform client_credentials request to SSO provider to obtain access token
        def _inner(sso_provider, scope):
            headers = {
                'Accept': 'application/json',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Cache-Control': 'no-cache'
            }
            data = f'grant_type=client_credentials&scope={scope}'
            try:
                r = requests.post(sso_provider, headers=headers, data=data, auth=(client_id, client_secret), verify=False, timeout=15)
                r.raise_for_status()
                js = r.json()
                return js.get('access_token')
            except RequestException:
                return None

        # default provider and scope (keeps original behavior)
        default_provider = 'https://delphix.okta.com/oauth2/default/v1/token'
        default_scope = 'groups'

        # allow override via environment (DX_SSO_PROVIDER, DX_SSO_SCOPE) or passed args
        # caller may pass via kwargs on this method; check for presence by retrieving from locals
        # but simpler: allow env override
        env_provider = os.environ.get('DX_SSO_PROVIDER')
        env_scope = os.environ.get('DX_SSO_SCOPE')

        provider = env_provider if env_provider else default_provider
        scope = env_scope if env_scope else default_scope

        return _inner(provider, scope)

    def generateSupportBundle(self, filename, bundle_type=None, analytics=None):
        op = 'resources/json/delphix/support/bundle'
        payload = {
            "type": "SupportBundleDownloadParameters",
            "path": filename,
        }
        if bundle_type:
            payload["bundleType"] = bundle_type
        if analytics:
            payload["includeAnalytics"] = True
        result, _fmt, rc = self.postJSONData(op, payload)
        if rc or result.get('status') != 'OK':
            return 1
        return 0

    def uploadSupportBundle(self, case=None, bundle_type=None, analytics=None):
        op = 'resources/json/delphix/support/bundle/upload'
        payload = {
            "type": "SupportBundleParameters",
        }
        if case:
            payload["caseNumber"] = case
        if bundle_type:
            payload["bundleType"] = bundle_type
        if analytics:
            payload["includeAnalytics"] = True
        result, _fmt, rc = self.postJSONData(op, payload)
        if rc or result.get('status') != 'OK':
            return None
        return result.get('job')
