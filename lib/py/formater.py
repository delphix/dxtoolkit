"""Minimal Formater port to support output formatting used by dx_ctl_analytics
"""
from collections import OrderedDict
import json
import sys


class Formater:
    def __init__(self, debug=None):
        self._lines = []
        self._debug = debug
        self._format = None
        self._header = []
        self._sepline = ''

    def addHeader(self, *columns):
        fmt = ''
        sepline = ''
        header = []
        for col in columns:
            # col is expected as dict {name: size}
            for k, v in col.items():
                header.append(k)
                fmt += "%-{}s ".format(v)
                sepline += '-' * v + ' '
        fmt = fmt + '\n'
        self._format = fmt
        self._header = header
        self._sepline = sepline

    def addLine(self, *columns):
        self._lines.append(list(columns))

    def addLineRev(self, *columns):
        self._lines.insert(0, list(columns))

    def print(self, nohead=None, fd=None):
        if fd is None:
            out = sys.stdout
        else:
            out = fd
        print('', file=out)
        if not nohead:
            if self._format:
                try:
                    print(self._format % tuple(self._header), file=out, end='')
                except Exception:
                    print(' '.join(self._header), file=out)
            print(self._sepline, file=out)
        for line in self._lines:
            try:
                print(self._format % tuple(line), file=out, end='')
            except Exception:
                print(' '.join(map(str, line)), file=out)

    def savejson(self, fd=None):
        out = fd or sys.stdout
        results = []
        for line in self._lines:
            obj = OrderedDict()
            for i, v in enumerate(line):
                key = self._header[i] if i < len(self._header) else str(i)
                obj[key] = v
            results.append(obj)
        print(json.dumps({'results': results}, indent=2), file=out)

    def savecsv(self, nohead=None, fd=None):
        out = fd or sys.stdout
        if not nohead:
            print('#' + ','.join(self._header), file=out)
        for line in self._lines:
            trimmed = [str(x).strip() for x in line]
            print(','.join(trimmed), file=out)

    def sortbynumcolumn(self, idx):
        """Sort stored lines by numeric column index (0-based)."""
        try:
            self._lines.sort(key=lambda row: float(row[idx]))
        except Exception:
            # Leave unsorted if conversion fails
            pass

    def getHeaderSize(self):
        return len(self._header)
