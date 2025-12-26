"""Base Analytic object (minimal subset used by dx_ctl_analytics)
"""
import json

from formater import Formater
from toolkit_helpers import convert_from_utc


class AnalyticObj:
    def __init__(self, dlpx, name, reference, type_, collectionAxes, collectionInterval, statisticType, debug=None):
        self._dlpx = dlpx
        self._name = name
        self._reference = reference
        self._type = type_
        self._collectionAxes = collectionAxes or []
        self._collectionInterval = collectionInterval
        self._statisticType = statisticType
        self._debug = debug
        # Get timezone from engine (like Perl does)
        self._detimezone = dlpx.getTimezone() if dlpx else None
        if debug:
            print(f"DEBUG: AnalyticObj init - timezone = {self._detimezone}")

    def getName(self):
        ret = self._name
        ret = ret.replace('default.', '')
        return ret

    def getAxes(self):
        return ','.join(self._collectionAxes)

    def getState(self):
        op = f"resources/json/delphix/analytics/{self._reference}"
        result, fmt, rc = self._dlpx.getJSONResult(op)
        if rc == 0 and result.get('status') == 'OK':
            return result['result'].get('state')
        return None

    def delete_analytic(self):
        op = f"resources/json/delphix/analytics/{self._reference}/delete"
        result, fmt, rc = self._dlpx.postJSONData(op, '{}')
        if rc or result.get('status') != 'OK':
            print(f"Error: {result.get('error') if isinstance(result, dict) else result}")
            return 1
        else:
            print(f"Analytic {self._name}  has been deleted")
            return 0

    def pause_analytic(self):
        op = f"resources/json/delphix/analytics/{self._reference}/pause"
        result, fmt, rc = self._dlpx.postJSONData(op, '{}')
        if rc or result.get('status') != 'OK':
            print(f"Error: {result.get('error') if isinstance(result, dict) else result}")
            return 1
        else:
            print(f"Analytic {self._name} has been stopped")
            return 0

    def resume_analytic(self):
        op = f"resources/json/delphix/analytics/{self._reference}/resume"
        result, fmt, rc = self._dlpx.postJSONData(op, '{}')
        if rc or result.get('status') != 'OK':
            print(f"Error: {result.get('error') if isinstance(result, dict) else result}")
            return 1
        else:
            print(f"Analytic {self._name} has been started")
            return 0

    # Generic analytics data fetcher for types without specialized classes
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
        rows = []
        timezone = getattr(self, '_detimezone', None)

        # Preserve common fields attached to each datapoint stream
        common_keys = ('client', 'interface', 'device', 'op', 'cached', 'local', 'remote', 'type')

        for ds in datapointStreams:
            common = {k: ds.get(k) for k in common_keys if ds.get(k) is not None}
            for dp in ds.get('datapoints', []):
                ts_raw = dp.get('timestamp')
                ts = convert_from_utc(ts_raw, timezone) if ts_raw else ''
                if resolution == 'H' and ts:
                    ts = ts[:13] + ts[13:]
                elif resolution == 'M' and ts:
                    ts = ts[:16] + ts[16:]

                row = {'timestamp': ts}

                # Prefer declared collection axes first
                for ca in self._collectionAxes:
                    if ca in dp:
                        row[ca] = dp.get(ca)

                # Capture all remaining datapoint keys except timestamp
                for k, v in dp.items():
                    if k == 'timestamp':
                        continue
                    if k not in row:
                        row[k] = v

                for k, v in common.items():
                    row.setdefault(k, v)

                rows.append(row)

        self._rows = rows
        return 0

    # Generic renderer to Formater for analytics without bespoke processData
    def processData(self, aggregation, obj=None):  # pylint: disable=unused-argument
        rows = getattr(self, '_rows', [])

        # Derive field order: timestamp first, then keys in discovery order
        field_order = []
        for r in rows:
            for key in r.keys():
                if key not in field_order:
                    field_order.append(key)

        if 'timestamp' not in field_order:
            field_order.insert(0, 'timestamp')
        else:
            field_order = ['timestamp'] + [k for k in field_order if k != 'timestamp']

        # If there are no datapoints, fall back to the declared axes to emit headers
        if not rows:
            for ca in self._collectionAxes:
                if ca not in field_order:
                    field_order.append(ca)

        output = Formater()
        output.addHeader(*[{f: max(10, len(str(f)) + 2)} for f in field_order])

        if getattr(self, '_overflow', False):
            print("Please reduce a range. API is not able to provide all data.")

        for r in rows:
            output.addLine(*[r.get(f, '') for f in field_order])

        self._output = output

    def calculate_latency(self, latency_hash):
        """Calculate average latency from histogram bucket data.
        Ported from Perl Analytic_obj.pm calculate_latency method.
        """
        if not latency_hash or not isinstance(latency_hash, dict):
            return None
        
        sum_count = 0
        sum_latency = 0
        
        for key_str, value in latency_hash.items():
            # Handle special case for < 10000 bucket
            if key_str == "< 10000":
                key = 1000
            else:
                try:
                    key = int(key_str)
                except (ValueError, TypeError):
                    continue
            
            if key > 0:
                sum_count += int(value)
                import math
                base = int(math.log10(key) + 0.00000001)
                sub = 10 ** (base - 1) * 5
                part_latency = key + sub
                sum_latency += (part_latency * int(value))
        
        if sum_latency == 0 and sum_count == 0:
            return None
        
        latency = sum_latency / sum_count if sum_count > 0 else None
        
        if latency is not None:
            # Convert to milliseconds (from nanoseconds)
            return f"{latency / 1000000:.2f}"
        return None

    def aggregation(self, ts, aggregation_level, key, metric, value):
        """Track aggregation metrics like Perl: stores values per timestamp, key, metric.
        
        Perl uses this to collect values that are later aggregated as min/max/percentile.
        """
        if not hasattr(self, 'aggreg'):
            self.aggreg = {}
        # Store values in arrays for later aggregation
        # aggreg[timestamp][key][metric] = [list of values]
        if ts not in self.aggreg:
            self.aggreg[ts] = {}
        if key not in self.aggreg[ts]:
            self.aggreg[ts][key] = {}
        if metric not in self.aggreg[ts][key]:
            self.aggreg[ts][key][metric] = []
        
        # Append value to the list for this metric
        try:
            self.aggreg[ts][key][metric].append(float(value))
        except (ValueError, TypeError):
            pass

    def calc_percentile(self, arr, percentile):
        """Calculate percentile using nearest rank method (like Perl)."""
        if not arr or len(arr) == 0:
            return 0
        sorted_arr = sorted(arr)
        idx = int(round(percentile * (len(sorted_arr) - 1)))
        return sorted_arr[idx]

    def doAggregation_worker(self, metrics_str):
        """Process aggregations: compute min/max/85pct for each metric (like Perl).
        
        Args:
            metrics_str: comma-separated list of metrics (e.g., "utilization", "throughput_r,throughput_w")
        """
        if not hasattr(self, 'aggreg') or not self.aggreg:
            return
        
        metrics = [m.strip() for m in metrics_str.split(',')]
        timestamps = sorted(self.aggreg.keys())
        
        output = Formater()
        
        # Build header dynamically based on metrics
        header_parts = ['time']
        if timestamps and any(k != 'none' for k in self.aggreg[timestamps[0]].keys()):
            header_parts.append('client')
        
        for metric in metrics:
            header_parts.extend([f'{metric}_min', f'{metric}_max', f'{metric}_85pct'])
        
        output.addHeader(*[{h: 10} for h in header_parts])
        
        # Output aggregated data
        for ts in timestamps:
            for client in sorted(self.aggreg[ts].keys()):
                if client == 'none':
                    line = [ts]
                else:
                    line = [ts, client]
                
                for metric in metrics:
                    if metric in self.aggreg[ts][client]:
                        values = sorted(self.aggreg[ts][client][metric])
                        min_val = values[0]
                        max_val = values[-1]
                        pct_85 = self.calc_percentile(values, 0.85)
                        line.extend([f"{min_val:.2f}", f"{max_val:.2f}", f"{pct_85:.2f}"])
                    else:
                        line.extend(['N/A', 'N/A', 'N/A'])
                
                output.addLine(*line)
        
        self._output_aggregation = output

    def doAggregation(self):
        """Default doAggregation for analytics without specialized aggregation.
        This is called for analytics types that don't have a specialized doAggregation method.
        For base analytic types, just copy raw output as aggregation (no special stats).
        """
        # If no aggregation data was collected, just use the raw output as fallback
        if not hasattr(self, '_output_aggregation'):
            if hasattr(self, '_output'):
                self._output_aggregation = self._output
            else:
                self._output_aggregation = Formater()
