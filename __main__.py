#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import argparse
import datetime
import ftplib
import itertools
import logging
import os
import sys
from StringIO import StringIO

from orm import automap

logging.basicConfig(level=logging.DEBUG,
                    filename='/tmp/abaita.log',
                    filemode='a',
                    format='%(asctime)s.%(msecs)03d|%(levelname)-8s|%(name)s|%(filename)s:%(lineno)d|%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def print_day(date, times):

    times = sorted(times)
    print "[{}]".format(date)
    for t in times:
        print t.time()

    if len(times) == 4:
        hours = (times[3] - times[2]) + (times[1] - times[0])
        print 'Ore sgobbate: {}'.format(hours)

    else:
        if len(times) == 3 and date == date.today():
            hours = ((datetime.datetime.now() - times[2]) + (times[1] - times[0]))
            ape = "DAI CHE SI FA L'APE!!!!!!" if date.isoweekday() == 5 else ''
            print 'Siamo a: {} ore... {}'.format(datetime.timedelta(seconds=int(hours.total_seconds())), ape)
        else:
            if date != date.today():
                print "WARNING: non hai timbrato, sciocco!"

    print ""


def print_report(conf, args):

    database = args.database or conf.get('database', 'endpoint')
    CAbaita = automap('abaita', database)['abaita']

    badge = args.badge or conf.get('user', 'badge')
    logging.info(u"Printing report for badge {}".format(badge))

    if args.all:
        query = CAbaita.load(badge=badge).order_by(CAbaita.date)
    else:
        query = CAbaita.load(badge=badge).filter(CAbaita.date == datetime.date.today())

    for date, rows in itertools.groupby(query, key=lambda r: r.date):
        logging.debug(u"Printing report for day {}".format(date))
        print_day(date, [datetime.datetime.combine(date, r.time) for r in rows])



def scrape(conf, args):

    database = args.database or conf.get('database', 'endpoint')
    CAbaita = automap('abaita', database)['abaita']

    user = args.user or conf.get('server', 'user')
    address = args.address or conf.get('server', 'address')
    password = args.password or conf.get('server', 'password')
    filename = args.filename or conf.get('server', 'filename')

    whitelist = args.badge
    if not whitelist:
        whitelist = set(filter(None, map(str.strip, conf.get('whitelist', 'values').split())))

    logging.info(u"Scraping database. Whitelist: {}".format(whitelist))

    # region Download the file
    try:
        ftp = ftplib.FTP(address)
        ftp.login(user, password)
    except Exception as e:
        logging.exception(u"Could not login to the FTP server: {}".format(e))
        sys.exit(1)
    else:
        logging.debug(u"Login ok")

    try:
        r = StringIO()
        ftp.retrbinary('RETR {}'.format(filename), r.write)
    except Exception as e:
        logging.exception(u"Could not download the file from the FTP server: {}".format(e))
        sys.exit(2)
    else:
        logging.debug(u"Download ok")
    # endregion

    rows = filter(None, map(str.strip, r.getvalue().split('\n')))
    if not rows:
        logging.warning(u"No rows found")
        return

    data = {}
    for row in rows[1:]:
        time, badge, row_id, _ = row.split()

        badge = badge[:6]
        if badge not in whitelist:
            continue

        dt = datetime.datetime.strptime(row[9:23], "%Y%m%d%H%M%S")
        entrata = bool(int(time[-1]))
        date = dt.date()

        data[date, dt.time(), badge] = entrata, row

    logging.debug(u"Found {} rows".format(len(data)))

    # Avoid saving duplicate data to the database
    existing_data = CAbaita.load().filter(CAbaita.badge.in_(whitelist))
    for row in existing_data:
        pk = (row.date, row.time, row.badge)
        if pk in data:
            del data[pk]

    logging.info(u"Saving {} new rows to the database".format(len(data)))
    for (date, dt, badge), (entrata, raw) in data.iteritems():
        CAbaita(
            date=date,
            time=dt,
            badge=badge,
            entrata=entrata,
            raw=raw,
        ).save()
    CAbaita.commit()


if __name__ == '__main__':

    # Main config file
    conf = ConfigParser.ConfigParser()
    conf.read(os.path.expanduser('~/.abaita.rc'))

    # region argparse setup
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address')
    parser.add_argument('-f', '--filename')
    parser.add_argument('-p', '--password')
    parser.add_argument('-d', '--database')
    parser.add_argument('-u', '--user')

    subparsers = parser.add_subparsers()
    parser_scrape = subparsers.add_parser('scrape')
    parser_scrape.add_argument('-b', '--badge', type=str, nargs='+')
    parser_scrape.set_defaults(func=scrape)

    parser_print = subparsers.add_parser('print')
    parser_print.add_argument('badge', nargs='?')
    parser_print.add_argument('-a', '--all', action='store_true')
    parser_print.set_defaults(func=print_report)
    # endregion

    args = parser.parse_args(sys.argv[1:])
    logging.info(u"Starting abaita with arguments {}".format(args))
    args.func(conf, args)
