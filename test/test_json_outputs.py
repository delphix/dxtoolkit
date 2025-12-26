import os
import sys
import io

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))
sys.path.insert(0, os.path.join(ROOT, 'bin'))

# Signal exporters to use the mock engine via env var
os.environ['DXTOOLKIT_USE_MOCK'] = '1'

# Ensure fresh imports pick up the mock-based selection logic
sys.modules.pop('dx_get_analytics', None)
sys.modules.pop('dx_get_network_tests', None)

from dx_get_analytics import main as analytics_main
from dx_get_network_tests import main as net_main
from formater import Formater
import toolkit_helpers


def test_formater_json_output():
    f = Formater()
    f.addHeader({'a': 3}, {'b': 3})
    f.addLine('x', '1')
    buf = io.StringIO()
    toolkit_helpers.print_output(f, 'json', True, buf)
    s = buf.getvalue()
    assert 'results' in s
    assert 'x' in s


def test_dx_get_analytics_json_with_mock(tmp_path):
    # Provide a minimal dxtools.conf so Engine.load_config succeeds
    conf = tmp_path / 'dxtools.conf'
    conf.write_text('{"data":[{"hostname":"Mock","ip_address":"127.0.0.1","port":80,"protocol":"http","default":"true"}]}' )
    os.environ['DXTOOLKIT_CONF'] = str(conf)
    outdir = tmp_path / 'analytics'
    outdir.mkdir()
    rc = analytics_main(['-d', 'Mock', '-type', 'standard', '-i', '60', '-outdir', str(outdir), '-format', 'json'])
    assert rc == 0
    files = list(outdir.iterdir())
    # Expect at least cpu/network raw+aggregated
    assert any('cpu-raw.json' in f.name or 'cpu-aggregated.json' in f.name for f in files)
    assert any('network-raw.json' in f.name or 'network-aggregated.json' in f.name for f in files)


def test_dx_get_network_tests_json_with_mock(capsys):
    # Provide a minimal dxtools.conf so Engine.load_config succeeds
    import tempfile
    fd, path = tempfile.mkstemp()
    os.write(fd, b'{"data":[{"hostname":"Mock","ip_address":"127.0.0.1","port":80,"protocol":"http","default":"true"}]}')
    os.close(fd)
    os.environ['DXTOOLKIT_CONF'] = path
    rc = net_main(['-d', 'Mock', '-type', 'latency', '-remoteaddr', 'all', '-format', 'json'])
    assert rc == 0
    captured = capsys.readouterr()
    assert 'engine' in captured.out
    assert 'latency' not in captured.err
