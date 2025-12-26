from analytic_obj import AnalyticObj
from formater import Formater
from toolkit_helpers import convert_from_utc


class AnalyticIOObj(AnalyticObj):
    def __init__(self, dlpx, name, reference, type_, collectionAxes, collectionInterval, statisticType, debug=None):
        super().__init__(dlpx, name, reference, type_, collectionAxes, collectionInterval, statisticType, debug)

    def getData(self, additional_parms, resolution):
        op = f"resources/json/delphix/analytics/{self._reference}/getData?{additional_parms}"
        result, fmt, retcode = self._dlpx.getJSONResult(op)
        if retcode:
            return 1
        datapointStreams = result.get('result', {}).get('datapointStreams', [])
        if len(datapointStreams) < 1:
            return 2
        if result.get('status') != 'OK':
            return 3

        self._overflow = result.get('result', {}).get('overflow')

        resultset = {}
        timestampfix = None
        timezone = getattr(self, '_detimezone', None)

        for ds in datapointStreams:
            # device/client switch
            dc = 'none'
            cache = 'none'
            
            op_name = ds.get('op')
            self.op = op_name
            
            if 'client' in ds:
                dc = ds.get('client')
            elif 'device' in ds:
                dc = ds.get('device')
            
            if 'cached' in ds:
                cache = str(ds.get('cached'))

            for dp in ds.get('datapoints', []):
                zulutime = dp.get('timestamp')
                ts = convert_from_utc(zulutime, timezone)
                if resolution == 'H':
                    if timestampfix is None:
                        timestampfix = ts[13:]
                    ts = ts[:13] + timestampfix
                if resolution == 'M':
                    if timestampfix is None:
                        timestampfix = ts[16:]
                    ts = ts[:16] + timestampfix

                row = {}
                for ca in self._collectionAxes:
                    v = dp.get(ca)
                    if isinstance(v, dict):
                        if ca in ('latency', 'size'):
                            row[ca] = v
                    elif v is not None:
                        row[ca] = v

                if op_name:
                    if ts not in resultset:
                        resultset[ts] = {}
                    if dc not in resultset[ts]:
                        resultset[ts][dc] = {}
                    if cache not in resultset[ts][dc]:
                        resultset[ts][dc][cache] = {}
                    resultset[ts][dc][cache][op_name] = row
                else:
                    resultset[ts] = row

        self.resultset = resultset
        return 0

    def doAggregation(self):
        if 'nfs-all' in self._name:
            # Include IOPS metrics for NFS to match Perl aggregated outputs
            self.doAggregation_worker('throughput_r,throughput_w,throughput_t,latency_r,latency_w,cache_hit_ratio,iops_r,iops_w,iops')
        else:
            self.doAggregation_worker('throughput_r,throughput_w,throughput_t,latency_r,latency_w,iops_r,iops_w,iops')

    def processData(self, aggregation, obj=None):
        # Match Perl's Analytic_io_obj.pm output format exactly
        self.aggreg = {}
        output = Formater()

        resultset = self.resultset
        if not resultset:
            self._output = output
            return

        timestamps = sorted(resultset.keys())
        
        # Debug: print structure for first timestamp
        if self._debug and self._debug >= 2 and timestamps:
            first_ts = timestamps[0]
            print(f"DEBUG processData: First timestamp = {first_ts}")
            print(f"DEBUG processData: resultset keys = {list(resultset.keys())[:3]}")
            print(f"DEBUG processData: First ts data = {resultset[first_ts]}")
        
        # Determine header based on analytic name and data structure
        if 'nfs-by-client' in self._name:
            output.addHeader({'timestamp': 20}, {'client': 20}, {'read_throughput': 20}, {'write_throughput': 20}, {'total_throughput': 20}, {'read_latency': 20}, {'write_latency': 20}, {'ops_read': 20}, {'ops_write': 20}, {'total_ops': 20})
        elif 'nfs-all' in self._name:
            output.addHeader({'timestamp': 20}, {'client': 20}, {'read_throughput': 20}, {'write_throughput': 20}, {'total_throughput': 20}, {'read_latency': 10}, {'write_latency': 10}, {'read_cache_hit_ratio': 10}, {'ops_read': 20}, {'ops_write': 20}, {'total_ops': 20})
        elif 'default.nfs' in self._name:
            output.addHeader({'timestamp': 20}, {'read_throughput': 20}, {'write_throughput': 20}, {'total_throughput': 20}, {'read_latency': 20}, {'write_latency': 20}, {'ops_read': 20}, {'ops_write': 20}, {'total_ops': 20})
        else:
            # Disk/iSCSI output - check if we have client-level data
            # Keys are 'none' (string) or None when there's no client, not Python None
            has_client_data = False
            if timestamps and isinstance(resultset.get(timestamps[0]), dict):
                first_ts_keys = list(resultset[timestamps[0]].keys())
                # If there are multiple keys OR the single key is not 'none'/None, we have client breakdown
                if len(first_ts_keys) > 1 or (len(first_ts_keys) == 1 and first_ts_keys[0] != 'none' and first_ts_keys[0] is not None):
                    has_client_data = True
            
            if has_client_data:
                output.addHeader({'timestamp': 20}, {'client': 20}, {'read_throughput': 20}, {'write_throughput': 20}, {'total_throughput': 20}, {'ops_read': 10}, {'ops_write': 10}, {'total_ops': 10}, {'read_latency': 10}, {'write_latency': 10})
            else:
                output.addHeader({'timestamp': 20}, {'read_throughput': 20}, {'write_throughput': 20}, {'total_throughput': 20}, {'ops_read': 10}, {'ops_write': 10}, {'total_ops': 10}, {'read_latency': 10}, {'write_latency': 10})

        if getattr(self, '_overflow', False):
            print("Please reduce a range. API is not able to provide all data.")
            print(f"min date {timestamps[0]} max date {timestamps[-1]}")

        for ts in timestamps:
            # Data structure is {ts: {dc: {cache: {op: data}}}}
            # dc (device/client) can be 'none' string, None, or an IP/device name
            # cache is 'none' string or '0'/'1'
            # First check if this timestamp has nested structure or flat
            ts_data = resultset[ts]
            
            if not isinstance(ts_data, dict):
                continue
                
            # Get all keys at dc level - could be None, 'none', or actual client IDs
            dc_keys = list(ts_data.keys())
            
            # Check if this is simple structure (single 'none' or None key with no client breakdown)
            is_simple = (len(dc_keys) == 1 and (dc_keys[0] == 'none' or dc_keys[0] is None))
            
            if is_simple:
                # Process single entry (no client breakdown)
                dc_data = ts_data[dc_keys[0]]
                
                read_throughput = 0
                write_throughput = 0
                read_iops = 0
                write_iops = 0
                r_latency = 'N/A'
                w_latency = 'N/A'

                # The nested structure could be {'none': {'read': ..., 'write': ...}} or {'None': {'read': ..., 'write': ...}}
                # or even {cache_key: {op: data}}
                if isinstance(dc_data, dict):
                    # Find the cache level - could be 'none', 'None', '0', '1', etc.
                    for cache_key in dc_data.keys():
                        ops_data = dc_data[cache_key]
                        if isinstance(ops_data, dict):
                            if 'read' in ops_data:
                                read_throughput += ops_data['read'].get('throughput', 0)
                                read_iops += ops_data['read'].get('count', 0)
                                if 'latency' in ops_data['read'] and ops_data['read']['latency']:
                                    r_latency = self.calculate_latency(ops_data['read']['latency'])
                            if 'write' in ops_data:
                                write_throughput += ops_data['write'].get('throughput', 0)
                                write_iops += ops_data['write'].get('count', 0)
                                if 'latency' in ops_data['write'] and ops_data['write']['latency']:
                                    w_latency = self.calculate_latency(ops_data['write']['latency'])

                # Convert throughput to MB
                read_tp_mb = read_throughput/(1024*1024)
                write_tp_mb = write_throughput/(1024*1024)
                total_tp_mb = (read_throughput+write_throughput)/(1024*1024)
                
                # Remove .000 from timestamp to match Perl
                ts_clean = ts.replace('.000', '')
                
                # Track aggregation - use date from timestamp
                ts_date = ts_clean.split()[0] if ts_clean else ''
                if aggregation:
                    self.aggregation(ts_date, aggregation, 'none', 'throughput_r', read_tp_mb)
                    self.aggregation(ts_date, aggregation, 'none', 'throughput_w', write_tp_mb)
                    self.aggregation(ts_date, aggregation, 'none', 'throughput_t', total_tp_mb)
                    # Aggregate IOPS for NFS as well to achieve parity
                    self.aggregation(ts_date, aggregation, 'none', 'iops_r', read_iops)
                    self.aggregation(ts_date, aggregation, 'none', 'iops_w', write_iops)
                    self.aggregation(ts_date, aggregation, 'none', 'iops', read_iops+write_iops)
                    if r_latency != 'N/A':
                        self.aggregation(ts_date, aggregation, 'none', 'latency_r', r_latency)
                    if w_latency != 'N/A':
                        self.aggregation(ts_date, aggregation, 'none', 'latency_w', w_latency)
                
                read_tp_mb_str = f"{read_tp_mb:.2f}"
                write_tp_mb_str = f"{write_tp_mb:.2f}"
                total_tp_mb_str = f"{total_tp_mb:.2f}"

                # Output without client column
                if 'nfs' in self._name:
                    output.addLine(ts_clean, read_tp_mb_str, write_tp_mb_str, total_tp_mb_str, r_latency, w_latency, read_iops, write_iops, read_iops+write_iops)
                else:
                    output.addLine(ts_clean, read_tp_mb_str, write_tp_mb_str, total_tp_mb_str, read_iops, write_iops, read_iops+write_iops, r_latency, w_latency)
            else:
                # Process entries with client keys
                dc_keys = sorted(resultset[ts].keys()) if isinstance(resultset[ts], dict) else []
                
                for dc_cur in dc_keys:
                    dc = resultset[ts][dc_cur]
                    
                    read_throughput = 0
                    write_throughput = 0
                    read_iops = 0
                    write_iops = 0
                    r_latency = 'N/A'
                    w_latency = 'N/A'

                    # Handle cached and non-cached data structures
                    if isinstance(dc, dict) and ('1' in dc or '0' in dc):
                        # Cached metrics
                        for cache_key in ['1', '0']:
                            if cache_key in dc:
                                cache_data = dc[cache_key]
                                if 'read' in cache_data:
                                    read_throughput += cache_data['read'].get('throughput', 0)
                                    read_iops += cache_data['read'].get('count', 0)
                                if 'write' in cache_data:
                                    write_throughput += cache_data['write'].get('throughput', 0)
                                    write_iops += cache_data['write'].get('count', 0)

                    # Convert throughput to MB
                    read_tp_mb = read_throughput/(1024*1024)
                    write_tp_mb = write_throughput/(1024*1024)
                    total_tp_mb = (read_throughput+write_throughput)/(1024*1024)
                    
                    # Track aggregation - use date from timestamp
                    ts_date = ts.replace('.000', '').split()[0] if ts else ''
                    if aggregation:
                        self.aggregation(ts_date, aggregation, dc_cur, 'throughput_r', read_tp_mb)
                        self.aggregation(ts_date, aggregation, dc_cur, 'throughput_w', write_tp_mb)
                        self.aggregation(ts_date, aggregation, dc_cur, 'throughput_t', total_tp_mb)
                        # Aggregate IOPS for NFS client metrics as well
                        self.aggregation(ts_date, aggregation, dc_cur, 'iops_r', read_iops)
                        self.aggregation(ts_date, aggregation, dc_cur, 'iops_w', write_iops)
                        self.aggregation(ts_date, aggregation, dc_cur, 'iops', read_iops+write_iops)
                        if r_latency != 'N/A':
                            self.aggregation(ts_date, aggregation, dc_cur, 'latency_r', r_latency)
                        if w_latency != 'N/A':
                            self.aggregation(ts_date, aggregation, dc_cur, 'latency_w', w_latency)
                    
                    read_tp_mb_str = f"{read_tp_mb:.2f}"
                    write_tp_mb_str = f"{write_tp_mb:.2f}"
                    total_tp_mb_str = f"{total_tp_mb:.2f}"

                    # Remove .000 from timestamp to match Perl
                    ts_clean = ts.replace('.000', '')

                    # Output with client column
                    if 'nfs' in self._name:
                        output.addLine(ts_clean, dc_cur, read_tp_mb_str, write_tp_mb_str, total_tp_mb_str, r_latency, w_latency, read_iops, write_iops, read_iops+write_iops)
                    else:
                        output.addLine(ts_clean, dc_cur, read_tp_mb_str, write_tp_mb_str, total_tp_mb_str, read_iops, write_iops, read_iops+write_iops, r_latency, w_latency)

        self._output = output
