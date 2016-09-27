#!/usr/bin/env python3
# -*- coding: utf8 -*-

import os
import sys
import zipfile


def unzip(filename):
    if zipfile.is_zipfile(filename):
        zf = zipfile.ZipFile(filename)
        for name in zf.namelist():
            data = zf.read(name)
            fo = open(name.encode('cp437').decode('gbk'), 'wb')
            fo.write(data)
            fo.close()
        zf.close()
    else:
        print("Error: {} is not a valid ZIP file".format(os.path.basename(filename)))


def main():
    if len(sys.argv[1:]) != 1:
        print('Usage: unzip.py [filename]')
    else:
        filename = sys.argv[1]
        unzip(filename)


if __name__ == '__main__':
    main()
