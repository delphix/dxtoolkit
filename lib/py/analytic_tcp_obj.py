from analytic_obj import AnalyticObj
from formater import Formater
from toolkit_helpers import convert_from_utc
from environment_obj import EnvironmentObj


class AnalyticTCPObj(AnalyticObj):
    """
    TCP Analytics Object
    Line-by-line conversion from Perl: lib/Analytic_tcp_obj.pm
    """
    
    def __init__(self, dlpx, name, reference, type_, collectionAxes, collectionInterval, statisticType, debug=None):
        """Constructor - Perl line 39-59"""
        super().__init__(dlpx, name, reference, type_, collectionAxes, collectionInterval, statisticType, debug)
        self._env = EnvironmentObj(dlpx, debug)

    def getData(self, additional_parms, resolution):
        """
        Fetch TCP analytics data - Perl lib/Analytic_tcp_obj.pm lines 64-190
        Key: Extract stream-level fields, build timestamp->remoteAddress->type->metrics structure
        """
        op = f"resources/json/delphix/analytics/{self._reference}/getData?{additional_parms}"
        result, fmt, retcode = self._dlpx.getJSONResult(op)
        
        if retcode:
            return 1
        
        if len(result.get('result', {}).get('datapointStreams', [])) < 1:
            return 2
        
        if result.get('status') != 'OK':
            return 3

        self._overflow = result.get('result', {}).get('overflow')
        resultset = {}
        timezone = getattr(self, '_detimezone', None)
        
        # Get JDBC ports for protocol classification
        jdbcports = self._env.getAllEnvironmentListenersPorts()

        # Process each datapoint stream
        for ds in result.get('result', {}).get('datapointStreams', []):
            
            # CRITICAL: Extract stream-level fields (NOT datapoint level!)
            # Perl lines 116-128
            # Connection-level data has remoteAddress, localPort, remotePort
            # Service-level data has 'service' field instead
            remoteAddress = ds.get('remoteAddress')
            remotePort = ds.get('remotePort')
            localPort = ds.get('localPort')
            service = ds.get('service')
            
            if self._debug and self._debug >= 2:
                print(f"DEBUG: Stream keys: {list(ds.keys())}")
                print(f"DEBUG: remoteAddress={remoteAddress}, localPort={localPort}, remotePort={remotePort}, service={service}")
            
            # Use service as identifier when connection fields aren't available
            if remoteAddress is None and service is not None:
                remoteAddress = service
                type_str = service
            else:
                # Build protocol type - Perl line 130
                type_str = f"{localPort}-{remotePort}"
                
                # Port classification - Perl lines 132-143
                if localPort in ('2049', '111', '4045'):
                    type_str = 'NFS traffic'
                elif remotePort == '8415':
                    type_str = 'Replication'
                elif remotePort in jdbcports:
                    type_str = 'JDBC'
                elif localPort in ('8341', '8415', '873'):
                    type_str = 'Snapsync'
                elif remotePort == '22':
                    type_str = 'SSH traffic'

            # Filter ports - Perl line 144
            # For service-level data, skip filtering (process all)
            should_process = True
            if localPort is not None and localPort in ('80', '22', '443', '5432'):
                should_process = False
            
            if should_process:
                
                # Process each datapoint in this stream
                for dp in ds.get('datapoints', []):
                    
                    zulutime = dp.get('timestamp')
                    ts = convert_from_utc(zulutime, timezone)
                    
                    # Apply resolution truncation - Perl lines 152-165
                    if resolution == 'H':
                        ts = ts[:13] + ts[13:]
                    elif resolution == 'M':
                        ts = ts[:16] + ts[16:]
                    
                    # Extract collection axes into row - Perl lines 167-173
                    row = {}
                    for ca in self._collectionAxes:
                        if ca in dp:
                            row[ca] = dp.get(ca)
                    
                    # Build 3-level nested structure - Perl line 179
                    # timestamp -> remoteAddress -> type -> metrics
                    if ts not in resultset:
                        resultset[ts] = {}
                    if remoteAddress not in resultset[ts]:
                        resultset[ts][remoteAddress] = {}
                    resultset[ts][remoteAddress][type_str] = row

        self.resultset = resultset
        return 0

    def doAggregation(self):
        """Compute aggregated statistics - Perl line 280"""
        self.doAggregation_worker('inBytes,outBytes')

    def processData(self, aggregation, obj=None):
        """
        Process raw TCP data - Perl lib/Analytic_tcp_obj.pm lines 193-268
        Key: Iterate timestamp -> client -> protocol, output metrics, track aggregation
        """
        resultset = getattr(self, 'resultset', {})
        output = Formater()
        
        # Add headers - Perl lines 205-218 (modern API with roundTripTime)
        output.addHeader(
            {'timestamp': 20},
            {'client': 20},
            {'protocol': 20},
            {'inBytes': 20},
            {'outBytes': 20},
            {'inUnorderedBytes': 20},
            {'retransmittedBytes': 20},
            {'unacknowledgedBytes': 20},
            {'congestionWindowSize': 20},
            {'roundTripTime': 20}
        )
        
        # Overflow warning - Perl lines 223-225
        if getattr(self, '_overflow', False):
            print("Please reduce a range. API is not able to provide all data.")
        
        # Process each timestamp, client, protocol - Perl lines 227-264
        for ts in sorted(resultset.keys()):
            
            for client_cur in sorted(resultset[ts].keys()):
                
                for type_cur in sorted(resultset[ts][client_cur].keys()):
                    
                    cur_line = resultset[ts][client_cur][type_cur]
                    
                    # Extract metric values - Perl lines 239-244
                    in_bytes = cur_line.get('inBytes')
                    out_bytes = cur_line.get('outBytes')
                    inUnorderedBytes = cur_line.get('inUnorderedBytes')
                    retransmittedBytes = cur_line.get('retransmittedBytes')
                    unacknowledgedBytes = cur_line.get('unacknowledgedBytes')
                    congestionWindowSize = cur_line.get('congestionWindowSize')
                    rtt = cur_line.get('roundTripTime')
                    
                    # Track aggregation - Perl lines 245-248
                    # Key: "IP-protocol" format for client aggregation
                    # Skip None values
                    if in_bytes is not None:
                        self.aggregation(ts, aggregation, f"{client_cur}-{type_cur}", 'inBytes', in_bytes)
                    if out_bytes is not None:
                        self.aggregation(ts, aggregation, f"{client_cur}-{type_cur}", 'outBytes', out_bytes)
                    
                    # Output raw data row - Perl line 254
                    output.addLine(ts, client_cur, type_cur, in_bytes, out_bytes,
                                  inUnorderedBytes, retransmittedBytes, unacknowledgedBytes,
                                  congestionWindowSize, rtt)
        
        self._output = output
