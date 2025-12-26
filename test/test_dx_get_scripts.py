import os
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))
sys.path.insert(0, os.path.join(ROOT, 'bin'))

from formater import Formater
from dx_get_analytics import aggregate_formater
from dx_get_network_tests import parse_args as parse_net_args
from dx_get_analytics import parse_args as parse_an_args


def test_aggregate_formater_sums_numeric_columns():
    f = Formater()
    f.addHeader({'timestamp': 10}, {'val1': 5}, {'val2': 5})
    f.addLine('2025-01-01 00:00:00', '1.0', '2.0')
    f.addLine('2025-01-01 00:00:00', '3.0', '4.0')
    f.addLine('2025-01-01 00:01:00', '5.0', '6.0')
    agg = aggregate_formater(f)
    # Should produce two rows with summed values
    lines = getattr(agg, '_lines', [])
    assert len(lines) == 2
    # First timestamp sums to 4.00 and 6.00
    assert lines[0][1] == '4.00'
    assert lines[0][2] == '6.00'


def test_parse_net_args_basic():
    args = parse_net_args(['-type', 'latency', '-remoteaddr', 'all'])
    assert args.test_type == 'latency'
    assert args.remoteaddr == 'all'


def test_parse_an_args_basic():
    args = parse_an_args(['-type', 'standard', '-outdir', '/tmp', '-i', '60'])
    assert args.atype == 'standard'
    assert args.outdir == '/tmp'
    assert args.interval == '60'
