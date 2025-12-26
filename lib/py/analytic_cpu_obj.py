from analytic_obj import AnalyticObj
from formater import Formater
from toolkit_helpers import convert_from_utc


class AnalyticCPUObj(AnalyticObj):
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
        timezone = getattr(self, '_detimezone', None)

        for ds in datapointStreams:
            for dp in ds.get('datapoints', []):
                zulutime = dp.get('timestamp')
                ts = convert_from_utc(zulutime, timezone)
                # normalize resolution to minute/hour as needed
                if resolution == 'H':
                    ts = ts[:13] + ts[13:]
                elif resolution == 'M':
                    ts = ts[:16] + ts[16:]

                row = {}
                for ca in self._collectionAxes:
                    if ca in dp:
                        row[ca] = dp.get(ca)

                resultset[ts] = row

        self.resultset = resultset
        return 0

    def doAggregation(self):
        # CPU aggregated metrics: compute min/max/85pct for utilization
        # Store aggregated data into _output_aggregation like Perl
        self.doAggregation_worker('utilization')

    def processData(self, aggregation, obj=None):
        resultset = getattr(self, 'resultset', {})
        
        output = Formater()
        # Match Perl output: timestamp and calculated util percentage
        output.addHeader({'timestamp': 20}, {'util': 10})

        if getattr(self, '_overflow', False):
            print("Please reduce a range. API is not able to provide all data.")

        for ts in sorted(resultset.keys()):
            data = resultset[ts]
            # Extract CPU component values
            kernel = data.get('kernel', 0)
            user = data.get('user', 0)
            idle = data.get('idle', 0)
            
            # Calculate utilization like Perl: (user+kernel)/(idle+user+kernel)*100
            ttl_cpu = idle + user + kernel
            util = 0.0 if ttl_cpu == 0 else ((user + kernel) / ttl_cpu) * 100
            
            # Track for aggregation (daily min/max/85pct)
            ts_date = ts.split()[0] if ts else ''
            self.aggregation(ts_date, 10, 'none', 'utilization', util)

            
            output.addLine(ts, f"{util:.2f}")

        self._output = output
