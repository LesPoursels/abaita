#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import argparse
import collections
import ftplib
import logging
import os
import sys
from StringIO import StringIO
from datetime import datetime, date

# This is the default for the lazy hacks who don't want to write the .abaita.rc file
your_code = 'put your badge number here'

logging.basicConfig(level=logging.DEBUG,
                    filename='/tmp/abaita.log',
                    filemode='a',
                    format='%(asctime)s.%(msecs)03d|%(levelname)-8s|%(name)s|%(filename)s:%(lineno)d|%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def safe_get(conf, section, option, default=None):
    # Fuck you ConfigParser.get
    return conf.get(section, option) if conf.has_option(section, option) else default


if __name__ == '__main__':

    # The config file overrides the CLI arguments
    conf = ConfigParser.ConfigParser()
    conf.read(os.path.expanduser('~/.abaita.rc'))
    ConfigParser.ConfigParser.safe_get = safe_get  # FUCK YEAH MONKEYPATCH

    # region argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--badge', default=your_code,
                        help='Your badge number')

    parser.add_argument('-f', '--filename', default='btransaction.loc',
                        help='Filename on the ftp server')

    parser.add_argument('-a', '--address', default='address')
    parser.add_argument('-u', '--user', default='admin')
    parser.add_argument('-p', '--password', default="password")

    args = parser.parse_args(sys.argv[1:])
    # endregion

    your_code = conf.safe_get('user', 'badge', args.badge)
    filename = conf.safe_get('server', 'filename', args.filename)

    try:
        ftp = ftplib.FTP(conf.safe_get('server', 'address', args.address))
        ftp.login(
            conf.safe_get('server', 'user', args.user),
            conf.safe_get('server', 'password', args.password)
        )
    except Exception as e:
        logging.exception(u"Could not login to the FTP server: {}".format(e))
        sys.exit(1)

    try:
        r = StringIO()
        ftp.retrbinary('RETR btransaction.loc', r.write)
    except Exception as e:
        logging.exception(u"Could not download the file from the FTP server: {}".format(e))
        sys.exit(1)

    rows = [row for row in r.getvalue().split('\n') if your_code in row]
    if not rows:
        logging.warning(u"No rows found for badge number {}".format(your_code))
        sys.exit(0)

    # Muahahaha
    months = collections.defaultdict(lambda: collections.defaultdict(list))

    cday = None
    ctime = None

    for i, row in enumerate(rows):
        dt = datetime.strptime(row[9:23], "%Y%m%d%H%M%S")

        if dt.date() != cday:
            cday = dt.date()
            print '\n[{}]'.format(str(cday))

        if ctime != dt.time():
            ctime = dt.time()
            months[(cday.year, cday.month)][cday.day].append(dt)
            print ctime
        else:
            continue

        times = months[(cday.year, cday.month)][cday.day]
        if len(times) == 4:
            hours = (times[3] - times[2]) + (times[1] - times[0])
            print 'Ore sgobbate: {}'.format(hours)

        if len(times) == 3 and cday == date.today():
            hours = (datetime.now() - times[2]) + (times[1] - times[0])
            ape = "DAI CHE SI FA L'APE!!!!!!" if cday.isoweekday() == 5 else ''
            print 'Siamo a: {} ore ... {}'.format(hours, ape)
