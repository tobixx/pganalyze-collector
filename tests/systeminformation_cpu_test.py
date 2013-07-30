#!/usr/bin/env python

import unittest
import os
import glob
import json
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), '..'))
from pganalyze_collector import SystemInformation


def linux_data_dir():
    return os.path.join(os.path.dirname(__file__), 'linux_cpuinfo_examples')


class TestLinuxCPUInfo(unittest.TestCase):
    def setUp(self):
        self.testfiles = glob.glob(linux_data_dir() + '/*.test')

    def test_cpuinfo_parser(self):
        for tf in self.testfiles:
            with open(tf) as f:
                lines = f.readlines()

            with open(os.path.splitext(tf)[0] + '.result') as f:
                reference = json.load(f)
            si = SystemInformation()
            result = si._parse_linux_cpu_cpuinfo(lines)

            self.assertEqual(reference, result)


if __name__ == '__main__': unittest.main()