from analytic_obj import AnalyticObj
from formater import Formater
from toolkit_helpers import convert_from_utc


class AnalyticNetworkObj(AnalyticObj):
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
            # Get network interface name
            nic = ds.get('networkInterface')
            if not nic:
                nic = 'none'
            
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
                    if ca in dp:
                        row[ca] = dp.get(ca)

                # Store by timestamp -> nic
                if ts not in resultset:
                    resultset[ts] = {}
                resultset[ts][nic] = row

        self.resultset = resultset
        return 0

    def doAggregation(self):
        # Network aggregated metrics: inBytes and outBytes (matching Perl)
        self.doAggregation_worker('inBytes,outBytes')

    def processData(self, aggregation, obj=None):
        # Match Perl's Analytic_network_obj.pm output format exactly
        output = Formater()
        
        resultset = self.resultset
        if not resultset:
            self._output = output
            return

        timestamps = sorted(resultset.keys())
        if not timestamps:
            self._output = output
            return
        
        # Build header dynamically like Perl does
        # Get list of NICs from first timestamp
        first_ts = timestamps[0]
        nics = sorted(resultset[first_ts].keys()) if first_ts in resultset else []
        
        # Build header list
        header_list = [
            {'timestamp': 20},
            {'inBytes': 20},
            {'outBytes': 20},
            {'inPackets': 20},
            {'outPackets': 20}
        ]
        
        # Add per-interface columns exactly like Perl
        for nic in nics:
            header_list.append({f'{nic}_inBytes': 20})
            header_list.append({f'{nic}_outBytes': 20})
            header_list.append({f'{nic}_inPackets': 20})
            header_list.append({f'{nic}_outPackets': 20})
        
        output.addHeader(*header_list)

        if getattr(self, '_overflow', False):
            print("Please reduce a range. API is not able to provide all data.")
            print(f"min date {timestamps[0]} max date {timestamps[-1]}")

        # Process each timestamp
        for ts in timestamps:
            # Remove .000 from timestamp to match Perl
            ts_clean = ts.replace('.000', '')
            
            # Calculate totals
            inBytes = 0
            outBytes = 0
            inPackets = 0
            outPackets = 0
            
            printarray = [ts_clean]
            nicarray = []
            
            for nic in nics:
                if nic in resultset[ts]:
                    nic_data = resultset[ts][nic]
                    nic_in = nic_data.get('inBytes', 0)
                    nic_out = nic_data.get('outBytes', 0)
                    nic_in_pkts = nic_data.get('inPackets', 0)
                    nic_out_pkts = nic_data.get('outPackets', 0)
                    
                    inBytes += nic_in
                    outBytes += nic_out
                    inPackets += nic_in_pkts
                    outPackets += nic_out_pkts
                    
                    nicarray.append(f"{int(nic_in)}")
                    nicarray.append(f"{int(nic_out)}")
                    nicarray.append(f"{int(nic_in_pkts)}")
                    nicarray.append(f"{int(nic_out_pkts)}")
            
            printarray.append(f"{int(inBytes)}")
            printarray.append(f"{int(outBytes)}")
            printarray.append(f"{int(inPackets)}")
            printarray.append(f"{int(outPackets)}")
            printarray.extend(nicarray)
            
            # Aggregation tracking (like Perl) - group by date
            ts_date = ts.split()[0] if ts else ''
            self.aggregation(ts_date, aggregation, 'none', 'inBytes', inBytes)
            self.aggregation(ts_date, aggregation, 'none', 'outBytes', outBytes)
            
            output.addLine(*printarray)

        self._output = output
