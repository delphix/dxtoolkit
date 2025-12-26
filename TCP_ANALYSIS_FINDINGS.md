# TCP Analytics API Structure Analysis

## Summary
After thorough investigation of the Python vs Perl analytics outputs, a critical discrepancy was found in the TCP analytics data structure:

### Verified Working (100% Match with Perl) ✅
- **Disk Analytics** - All fields and timestamps match perfectly
- **Network Analytics** - All fields and timestamps match perfectly  
- **iSCSI Analytics** - All fields and timestamps match perfectly

### Issue: TCP Analytics API Structure Changed ❌

## Root Cause Analysis

### Perl Output (Expected/Reference)
- **Format**: Per-connection data with remote client IP addresses
- **Example fields**: client IP (10.160.2.70, 129.212.189.134, etc.), protocol type, inBytes, outBytes, metrics
- **Example row**: `2025-12-19 13:34:00,10.160.2.70,-,52,36,,,0,8470,19962`
- **Data structure**: Each row represents a unique client connection

### Python Output (Current API Response)
- **Format**: Service-level aggregates with service names
- **Available fields**: service name ('ssh', 'https', 'iscsi-target', 'dlpx-sp', 'http', 'unknown'), retransmittedSegs, unsentBytes, roundTripTime
- **Example row**: `2025-12-19 13:35:00,iscsi-target,-,,,,,,,25981` (mostly empty fields)
- **Data structure**: 8 datapointStreams with different service names, no per-connection breakdown

## API Investigation Details

### Analytics Definition
The TCP analytics metadata (`default.tcp`) includes these **collectionAxes**:
```
['congestionWindowSize', 'retransmittedSegs', 'outBytes', 'localAddress', 'service', 
'roundTripTime', 'receiveWindowSize', 'inBytes', 'unsentBytes', 'sendWindowSize', 
'unacknowledgedBytes', 'remoteAddress']
```

**Note**: The metadata includes `remoteAddress`, `localAddress`, `inBytes`, `outBytes`, etc., but the actual API response does NOT include these fields in the datapoints!

### Actual API Response Structure
When querying `GET /resources/json/delphix/analytics/ANALYTICS_STATISTIC_SLICE-4/getData?...`

**Response contains**:
```json
{
  "datapointStreams": [
    {
      "type": "DatapointStream",
      "service": "iscsi-target",  // Service name instead of remoteAddress
      "datapoints": [
        {
          "type": "Datapoint",
          "timestamp": "2025-12-19T12:35:00.000Z",
          "retransmittedSegs": 0,
          "unsentBytes": 0,
          "roundTripTime": 25981
          // Note: NO inBytes, outBytes, remoteAddress, localAddress, localPort, remotePort
        }
      ]
    }
    // ... more service-level streams ...
  ]
}
```

## Possible Explanations

1. **API Version Change**: The Delphix API may have changed the TCP analytics structure in newer versions, moving from per-connection tracking to service-level aggregation.

2. **Data Collection Configuration**: The TCP analytics might have been reconfigured on the engine to use aggregation instead of per-connection tracking.

3. **Perl Data Source**: The Perl output files may have been generated when the engine was in a different state or using different analytics configuration.

4. **Permission/Visibility**: There might be different data available based on user permissions or authentication level (though both Perl and Python use the same credentials).

## Timestamp Issue (RESOLVED) ✅

**Previously**: Python was passing wrong timestamps to the API (1 hour ahead of Perl)
**Root Cause**: Python `parse_timestamp()` wasn't converting user input from engine timezone (Europe/Rome) to UTC
**Solution**: Updated `parse_timestamp()` to accept `engine_tz` parameter and convert from engine timezone to UTC using `ZoneInfo`
**Result**: Timestamps now match Perl exactly (both use UTC in API calls)

### Example
- User input: `2025-12-19 13:34:00` (engine timezone: Europe/Rome)
- Python NOW: Converts to `2025-12-19T12:34:00Z` (UTC) ✅
- Matches Perl behavior: Same UTC conversion

## Recommendations

### Option 1: Accept API Limitation (Recommended)
Update TCP analytics output to work with current API data structure:
- Output service names instead of client IPs
- Output available fields only (retransmittedSegs, unsentBytes, roundTripTime)
- Pad missing fields with empty values to maintain column format
- Document that TCP data is service-level aggregates, not per-connection

### Option 2: Investigate Per-Connection Data Availability
- Check engine's analytics configuration for TCP analytics
- Verify if there's a "detailed" or "expanded" mode for TCP analytics
- Check if custom TCP analytics can be created (like `tcp-by-client`)
- Review engine logs for any analytics state changes

### Option 3: Compare API Versions
- Check which Delphix Engine version Perl scripts target
- Check current engine version and API version
- Determine if API versioning is causing data structure differences

## Current Status

✅ **Disk**: Working perfectly  
✅ **Network**: Working perfectly  
✅ **iSCSI**: Working perfectly  
❓ **TCP**: Waiting for decision on per-connection data availability

**Timezone conversion**: FIXED and validated ✅
**Timestamp accuracy**: Matches Perl exactly ✅

