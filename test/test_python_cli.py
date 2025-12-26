#!/usr/bin/env python3
"""Minimal unit tests for Python dxtoolkit CLIs."""
import unittest
import sys
import os
import tempfile
import json
from unittest.mock import MagicMock, patch

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))

# Import CLI entry points
sys.path.insert(0, os.path.join(ROOT, 'bin'))

import toolkit_helpers
from capacity_obj import CapacityObj
from storage_obj import StorageObj
from databases import Databases
from group_obj import GroupObj
from namespace_obj import NamespaceObj


class TestToolkitHelpers(unittest.TestCase):
    """Test toolkit_helpers utilities."""

    def test_get_unit_label(self):
        """Test unit label formatting."""
        self.assertEqual(toolkit_helpers.get_unit('Size', 'G'), 'Size [GB]')
        self.assertEqual(toolkit_helpers.get_unit('Free', 'M'), 'Free [MB]')
        self.assertEqual(toolkit_helpers.get_unit('Capacity', 'T'), 'Capacity [TB]')

    def test_print_size_conversion(self):
        """Test size unit conversion."""
        # 1 GB to GB = 1.00
        self.assertEqual(toolkit_helpers.print_size(1, 'G', 'G'), '1.00')
        # 1024 MB to GB = 1.00
        self.assertEqual(toolkit_helpers.print_size(1024, 'M', 'G'), '1.00')
        # 0.5 TB to GB = 512.00
        self.assertEqual(toolkit_helpers.print_size(0.5, 'T', 'G'), '512.00')

    def test_convert_from_utc(self):
        """Test UTC timestamp conversion."""
        ts = '2025-12-20T10:30:45.000Z'
        result = toolkit_helpers.convert_from_utc(ts, drop_fraction=True)
        self.assertIn('2025-12-20 10:30:45', result)
        # Should strip 'Z' suffix
        self.assertFalse(result.endswith('Z'))

    def test_trim(self):
        """Test string trimming."""
        self.assertEqual(toolkit_helpers.trim('  hello  '), 'hello')
        self.assertIsNone(toolkit_helpers.trim(None))
        self.assertEqual(toolkit_helpers.trim(''), '')

    def test_version(self):
        """Test version string is set."""
        self.assertIsNotNone(toolkit_helpers.version)
        self.assertIn('.', toolkit_helpers.version)


class TestDatabases(unittest.TestCase):
    """Test Databases class."""

    def setUp(self):
        """Setup mock engine and databases."""
        self.mock_engine = MagicMock()
        self.mock_engine.getJSONResult = MagicMock(return_value=(
            {
                'status': 'OK',
                'result': [
                    {
                        'reference': 'DB-1',
                        'name': 'Oracle_master',
                        'type': 'OracleDatabaseContainer',
                        'provisionContainer': None,
                        'group': 'G-1',
                    },
                    {
                        'reference': 'VDB-1',
                        'name': 'Oracle_DEV',
                        'type': 'OracleDatabaseContainer',
                        'provisionContainer': 'PC-1',
                        'group': 'G-2',
                        'namespace': None,
                    },
                ]
            },
            None, 0
        ))

    @patch('databases.NamespaceObj')
    def test_get_db_by_type_vdb(self, mock_ns):
        """Test filtering VDBs."""
        db = Databases(self.mock_engine)
        vdbs = db.getDBByType('VDB')
        self.assertEqual(len(vdbs), 1)
        self.assertIn('VDB-1', vdbs)

    @patch('databases.NamespaceObj')
    def test_get_db_by_type_dsource(self, mock_ns):
        """Test filtering dSources."""
        db = Databases(self.mock_engine)
        sources = db.getDBByType('dSource')
        self.assertEqual(len(sources), 1)
        self.assertIn('DB-1', sources)

    @patch('databases.NamespaceObj')
    def test_get_db_list(self, mock_ns):
        """Test get all database list."""
        db = Databases(self.mock_engine)
        all_dbs = db.getDBList()
        self.assertEqual(len(all_dbs), 2)

    @patch('databases.NamespaceObj')
    def test_is_replica(self, mock_ns):
        """Test replica detection."""
        db = Databases(self.mock_engine)
        # DB-1 has no namespace, not a replica
        self.assertFalse(db.isReplica('DB-1'))
        # VDB-1 also has no namespace in our mock
        self.assertFalse(db.isReplica('VDB-1'))


class TestCapacityObj(unittest.TestCase):
    """Test CapacityObj class."""

    def setUp(self):
        """Setup mock engine."""
        self.mock_engine = MagicMock()
        self.mock_engine.getJSONResult = MagicMock(return_value=(
            {
                'status': 'OK',
                'result': [
                    {
                        'container': 'DB-1',
                        'breakdown': {
                            'actualSpace': 1024 * 1024 * 1024 * 1024,  # 1 TB
                            'activeSpace': 512 * 1024 * 1024 * 1024,   # 512 GB
                            'logSpace': 128 * 1024 * 1024 * 1024,      # 128 GB
                            'syncSpace': 256 * 1024 * 1024 * 1024,     # 256 GB
                            'unvirtualizedSpace': 0,
                        }
                    }
                ]
            },
            None, 0
        ))

    def test_load_databases(self):
        """Test loading capacity data."""
        cap = CapacityObj(self.mock_engine)
        cap.LoadDatabases()
        self.assertEqual(len(cap._databases), 1)

    def test_get_detailed_db_usage(self):
        """Test capacity metrics."""
        cap = CapacityObj(self.mock_engine)
        cap.LoadDatabases()
        usage = cap.getDetailedDBUsage('DB-1', None)
        # 1 TB = 1024 GB
        self.assertAlmostEqual(float(usage['totalsize']), 1024.0, places=1)
        # 512 GB
        self.assertAlmostEqual(float(usage['currentcopy']), 512.0, places=1)


class TestStorageObj(unittest.TestCase):
    """Test StorageObj class."""

    def setUp(self):
        """Setup mock engine."""
        self.mock_engine = MagicMock()
        self.mock_engine.getJSONResult = MagicMock(return_value=(
            {
                'status': 'OK',
                'result': [
                    {
                        'reference': 'ST-1',
                        'state': 'COMPLETED',
                        'startTime': '2025-12-20T10:00:00.000Z',
                        'testResults': [
                            {
                                'testName': 'Random 4K Read w/ 16 jobs',
                                'iops': 500,
                                'throughput': 2 * 1024 * 1024,  # 2 MB/s
                                'latencyGrade': 'A',
                                'averageLatency': 30.5,
                                'latency95thPercentile': 100.2,
                                'minLatency': 0.1,
                                'maxLatency': 500.0,
                                'stddevLatency': 50.0,
                            }
                        ]
                    }
                ]
            },
            None, 0
        ))
        self.mock_engine.getTimezone = MagicMock(return_value='UTC')

    def test_load_storage_test(self):
        """Test loading storage tests."""
        st = StorageObj(self.mock_engine)
        st.LoadStorageTest()
        self.assertEqual(len(st._storage_test), 1)

    def test_get_test_list(self):
        """Test listing tests."""
        st = StorageObj(self.mock_engine)
        st.LoadStorageTest()
        tests = st.getTestList()
        self.assertIn('ST-1', tests)

    def test_parse_test_results(self):
        """Test parsing test results."""
        st = StorageObj(self.mock_engine)
        st.LoadStorageTest()
        st.parseTestResults('ST-1')
        # Check that hash was created
        self.assertIn('_test_results_hash', st._storage_test['ST-1'])

    def test_get_latency_metrics(self):
        """Test latency metrics extraction."""
        st = StorageObj(self.mock_engine)
        st.LoadStorageTest()
        st.parseTestResults('ST-1')
        testname = 'Random 4K Read w/ 16 jobs'
        self.assertEqual(st.getLatencyGrade('ST-1', testname), 'A')
        self.assertAlmostEqual(float(st.getLatencyAvg('ST-1', testname)), 30.5, places=1)
        self.assertEqual(st.getTestIOPS('ST-1', testname), 500)


if __name__ == '__main__':
    unittest.main()
