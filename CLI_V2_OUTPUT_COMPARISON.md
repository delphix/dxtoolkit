# CLI v2 Output Comparison: Bash vs Python

## Summary

Both `cli_v2.sh` (v3.1.0, Perl-based) and `cli_v2.py` (v3.2.0, Python-based) healthcheck scripts now execute successfully against the Delphix engine `uvo16q97x1884fhr7oq.vm.cld.sr`.

## Output Structure

### Bash Script (cli_v2.sh)
- **Output directory**: `/tmp/cli_v2_out_sh/`
- **Subdirectories**: `analytics/`, `misc/`
- **Total files**: 6

### Python Script (cli_v2.py)
- **Output directory**: `/tmp/cli_v2_out_py/`
- **Subdirectories**: `analytics/`, `misc/`
- **Total files**: 5

## File-by-File Comparison

### Network Tests (Both produce):
| File | Bash | Python | Notes |
|------|------|--------|-------|
| `uvo16q97x1884fhr7oq.vm.cld.sr_NL.csv` | ✅ | ✅ | Network latency test results |
| `uvo16q97x1884fhr7oq.vm.cld.sr_NT.csv` | ✅ | ✅ | Network throughput test results |

**Issue**: Python network test CSVs are nearly empty (headers only) compared to bash which has actual test data.

### Analytics Files (Bash only):
| File | Bash | Python | Notes |
|------|------|--------|-------|
| `uvo16q97x1884fhr7oq.vm.cld.sr-analytics-cpu-raw.csv` | ✅ | ❌ | CPU utilization raw data |
| `uvo16q97x1884fhr7oq.vm.cld.sr-analytics-cpu-aggregated.csv` | ✅ | ❌ | CPU utilization aggregated |
| `uvo16q97x1884fhr7oq.vm.cld.sr-analytics-network-raw.csv` | ✅ | ❌ | Network analytics raw data |
| `uvo16q97x1884fhr7oq.vm.cld.sr-analytics-network-aggregated.csv` | ✅ | ❌ | Network analytics aggregated |

**Note**: Bash script notes "No data returned" for disk and nfs analytics (collector not running).

### Miscellaneous Data (Python only):
| File | Bash | Python | Notes |
|------|------|--------|-------|
| `uvo16q97x1884fhr7oq.vm.cld.sr_capacity.csv` | ❌ | ✅ | Storage capacity metrics |
| `uvo16q97x1884fhr7oq.vm.cld.sr_appliance.csv` | ❌ | ✅ | Appliance information |
| `uvo16q97x1884fhr7oq.vm.cld.sr_config.csv` | ❌ | ✅ | System configuration |

## Key Differences

### 1. Analytics Collection
- **Bash**: Uses Perl `dx_get_analytics.pl` which successfully collects CPU and network analytics
- **Python**: Uses Python `dx_get_analytics.py` but analytics files not created (may have failed silently)

### 2. Additional Data Collection
- **Bash**: Focused on analytics and network tests only
- **Python**: Collects additional system information (capacity, appliance config, IORC)

### 3. Network Test Results
- **Bash**: Network latency/throughput tests execute and populate CSV files with job results
- **Python**: Network tests appear to run but CSV files contain no data rows

## Recommendations

### For Python Script (cli_v2.py):
1. **Fix analytics collection**: Investigate why `dx_get_analytics.py` doesn't produce output files
2. **Fix network test output**: Debug why `dx_ctl_network_tests.py` produces empty CSVs
3. **Consider**: Add error handling to surface subprocess failures

### For Bash Script (cli_v2.sh):
1. **Optional enhancement**: Add capacity, appliance, and config collection to match Python version
2. **Alternative**: Document that Python version provides additional metrics

## Conclusion

- ✅ Both scripts connect successfully to the Delphix engine
- ✅ Both scripts execute their respective collection workflows
- ⚠️ Output completeness varies significantly between versions
- ⚠️ Python version needs debugging for analytics and network test output
- ℹ️ Python version collects more data types than bash (capacity, appliance, config)

## Test Execution Details

**Engine**: uvo16q97x1884fhr7oq.vm.cld.sr  
**Credentials**: admin/Delphix_123!  
**Protocol**: https/443  
**Bash script**: v3.1.0  
**Python script**: v3.2.0  
**Test date**: 2025-12-24
