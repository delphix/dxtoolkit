"""Toolkit helpers port (extended for new CLI parity)."""
import json
import os
from decimal import Decimal, ROUND_HALF_UP

version = '2.4.24.2'


def ensure_config_file(cfg_path):
    """Validate the provided config file path and emit a friendly error.
    
    Allows None to use defaults (DXTOOLKIT_CONF env or lib/py/dxtools.conf),
    but validates if an explicit path is provided.
    """
    if not cfg_path:
        # Allow None - will use defaults in Engine.load_config()
        return True
    if not os.path.exists(cfg_path):
        print(f"ERROR: config file not found: {cfg_path}")
        return False
    return True


def logger(debug, msg, verbose=None):
    """Lightweight logger that mirrors Perl semantics for verbosity levels."""
    if debug is None:
        return
    try:
        level = int(debug)
    except Exception:
        level = 0
    if verbose is not None:
        if level >= verbose:
            print('-', msg)
    else:
        print(msg)


def get_engine_list(all_flag, dx_host, engine_obj):
    if all_flag:
        return engine_obj.getAllEngines()
    elif dx_host:
        return [dx_host]
    else:
        return engine_obj.getDefaultEngines()


def print_output(output_obj, fmt, nohead, fd=None):
    target = fd or None
    if fmt and fmt.lower() == 'csv':
        output_obj.savecsv(nohead, target)
    elif fmt and fmt.lower() == 'json':
        output_obj.savejson(target)
    else:
        output_obj.print(nohead, target)


def trim(s):
    if s is None:
        return s
    return str(s).strip()


def get_unit(label, unit):
    """Return a label with unit suffix, mirroring Perl helper."""
    suffix = {
        'K': 'KB',
        'M': 'MB',
        'G': 'GB',
        'T': 'TB',
    }.get(str(unit).upper(), str(unit))
    return f"{label} [{suffix}]"


def _to_decimal(val):
    try:
        return Decimal(str(val))
    except Exception:
        return None


def print_size(value, from_unit, to_unit):
    """Convert sizes between units (K/M/G/T) and format with 2 decimals."""
    if value is None:
        return 'N/A'
    src = str(from_unit).upper()
    dst = str(to_unit).upper()
    src_map = {'K': Decimal(1), 'M': Decimal(1024), 'G': Decimal(1024) ** 2, 'T': Decimal(1024) ** 3}
    v = _to_decimal(value)
    if v is None or src not in src_map or dst not in src_map:
        return value
    bytes_val = v * src_map[src]
    out = bytes_val / src_map[dst]
    return f"{out.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}"


def convert_from_utc(timestamp, timezone=None, drop_fraction=None):
    """Convert UTC timestamp to target timezone.
    
    If timezone is None, just strips Z and replaces T with space.
    Otherwise, converts from UTC to the specified timezone.
    """
    if timestamp is None:
        return timestamp
    
    ts = str(timestamp)
    
    # If no timezone specified, just format without conversion
    if timezone is None:
        if drop_fraction:
            ts = ts.replace('.000Z', 'Z')
        ts = ts.replace('T', ' ')
        if ts.endswith('Z'):
            ts = ts[:-1]
        return ts
    
    # Parse the UTC timestamp
    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        # Python < 3.9
        import pytz
    
    # Remove .000 and Z for parsing
    ts_clean = ts.replace('.000Z', 'Z').replace('Z', '').replace('T', ' ')
    
    try:
        # Parse as UTC
        dt_utc = datetime.strptime(ts_clean, '%Y-%m-%d %H:%M:%S')
        
        # Convert to target timezone
        try:
            # Try Python 3.9+ zoneinfo first
            from zoneinfo import ZoneInfo
            dt_utc = dt_utc.replace(tzinfo=ZoneInfo('UTC'))
            dt_local = dt_utc.astimezone(ZoneInfo(timezone))
        except ImportError:
            # Fall back to pytz for older Python
            import pytz
            dt_utc = pytz.utc.localize(dt_utc)
            local_tz = pytz.timezone(timezone)
            dt_local = dt_utc.astimezone(local_tz)
        
        # Format without timezone info (just the local time)
        return dt_local.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        # If conversion fails, fall back to simple formatting
        if drop_fraction:
            ts = ts.replace('.000Z', 'Z')
        ts = ts.replace('T', ' ')
        if ts.endswith('Z'):
            ts = ts[:-1]
        return ts


def parse_timestamp(arg: str, engine_tz: str = None):
    """Parse flexible time argument formats used by Perl scripts.

    Supports:
    - ISO strings: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, YYYY-MM-DDTHH:MM:SSZ
    - Oracle-like: DD-MON-YYYY, DD-MON-YYYY HH24:MI:SS (MON is JAN,FEB,...)
    - Relative: -Xmin, -Xhours, -Xdays (e.g., -30min, -2days)

    If engine_tz is provided, input is interpreted as being in that timezone
    and converted to UTC (matching Perl behavior).

    Returns ISO-like 'YYYY-MM-DDTHH:MM:SSZ' string or None if parse fails.
    """
    if arg is None:
        return None
    import re
    from datetime import datetime, timedelta, timezone
    from zoneinfo import ZoneInfo

    s = str(arg).strip()
    # Relative formats
    m = re.fullmatch(r"-([0-9]+)(min|mins|minute|minutes|hour|hours|day|days)", s, re.IGNORECASE)
    if m:
        qty = int(m.group(1))
        unit = m.group(2).lower()
        delta = None
        if unit in ('min', 'mins', 'minute', 'minutes'):
            delta = timedelta(minutes=qty)
        elif unit in ('hour', 'hours'):
            delta = timedelta(hours=qty)
        else:
            delta = timedelta(days=qty)
        dt = datetime.now(timezone.utc) - delta
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    # ISO date/time
    # Normalize separators and trailing Z
    try_candidates = [
        s,
        s.replace(' ', 'T'),
        s.replace('Z', ''),
        s.replace('/', '-'),
    ]
    for cand in try_candidates:
        try:
            # if only date provided, assume midnight
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}$", cand):
                from datetime import date
                y, m2, d = cand.split('-')
                # Create datetime in engine timezone (if provided) or UTC
                if engine_tz:
                    try:
                        tz = ZoneInfo(engine_tz)
                        dt = datetime(int(y), int(m2), int(d), tzinfo=tz)
                    except Exception:
                        # Fallback to UTC if timezone is invalid
                        dt = datetime(int(y), int(m2), int(d), tzinfo=timezone.utc)
                else:
                    dt = datetime(int(y), int(m2), int(d), tzinfo=timezone.utc)
                # Convert to UTC
                return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            # Try full datetime
            dt_naive = datetime.fromisoformat(cand.replace('Z', ''))
            # Create datetime in engine timezone (if provided) or UTC
            if engine_tz:
                try:
                    tz = ZoneInfo(engine_tz)
                    dt = dt_naive.replace(tzinfo=tz)
                except Exception:
                    # Fallback to UTC if timezone is invalid
                    dt = dt_naive.replace(tzinfo=timezone.utc)
            else:
                dt = dt_naive.replace(tzinfo=timezone.utc)
            # Convert to UTC
            return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception:
            pass

    # DD-MON-YYYY [HH24:MI:SS]
    mon_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
    }
    m = re.fullmatch(r"(\d{2})-([A-Za-z]{3})-(\d{4})(?:\s+(\d{2}):(\d{2}):(\d{2}))?", s)
    if m:
        d = int(m.group(1))
        mon = mon_map.get(m.group(2).upper())
        y = int(m.group(3))
        hh = int(m.group(4) or 0)
        mi = int(m.group(5) or 0)
        ss = int(m.group(6) or 0)
        if mon:
            # Create datetime in engine timezone (if provided) or UTC
            if engine_tz:
                try:
                    tz = ZoneInfo(engine_tz)
                    dt = datetime(y, mon, d, hh, mi, ss, tzinfo=tz)
                except Exception:
                    # Fallback to UTC if timezone is invalid
                    dt = datetime(y, mon, d, hh, mi, ss, tzinfo=timezone.utc)
            else:
                dt = datetime(y, mon, d, hh, mi, ss, tzinfo=timezone.utc)
            # Convert to UTC
            return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    return None
    if m:
        d = int(m.group(1))
        mon = mon_map.get(m.group(2).upper())
        y = int(m.group(3))
        hh = int(m.group(4) or 0)
        mi = int(m.group(5) or 0)
        ss = int(m.group(6) or 0)
        if mon:
            dt = datetime(y, mon, d, hh, mi, ss, tzinfo=timezone.utc)
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    return None


def wait_for_job(engine_obj, jobref, success_msg=None, failure_msg=None, debug=None):
    """Blocking wait for a job; returns 0 on completed, 1 otherwise."""
    try:
        from jobs_obj import JobsObj
    except Exception:
        print("Missing JobsObj implementation")
        return 1

    job = JobsObj(engine_obj, jobref, silent='true', debug=debug)
    state = job.waitForJob()
    if state == 'COMPLETED':
        if success_msg:
            print(success_msg)
        return 0
    if failure_msg:
        print(failure_msg)
    else:
        print(f"Job {jobref} finished with state {state}")
    return 1


def get_dblist_from_filter(db_type, group, host, dbname, databases, groups, debug=None, dsource=None):
    """Minimal DB filter helper mirroring common Perl usage."""
    logger(debug, "Entering get_dblist_from_filter", 1)
    db_list = databases.getDBList()
    if host:
        host_filtered = databases.getDBForHost(host)
        db_list = [d for d in db_list if d in host_filtered]
    if dsource:
        parent_filtered = databases.getDBByParent(dsource)
        db_list = [d for d in db_list if d in parent_filtered]
    if db_type:
        type_filtered = databases.getDBByType(db_type)
        db_list = [d for d in db_list if d in type_filtered]
    if group:
        gref = groups.getGroupByName(group)
        gref = gref.get('reference') if gref else None
        if gref:
            grp_filtered = databases.getDBForGroup(gref)
            db_list = [d for d in db_list if d in grp_filtered]
        else:
            db_list = []
    if dbname:
        names = set(dbname.split(','))
        db_list = [d for d in db_list if databases.getName(d) in names]
    return db_list if db_list else None
