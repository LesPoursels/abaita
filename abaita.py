#!/usr/bin/env python


import ftplib
from datetime import datetime, date
from StringIO import StringIO


your_code = 'caccia-il-codice-QUI'
filename = 'btransaction.loc'
ftp = ftplib.FTP("address")
ftp.login("admin", "password")


r = StringIO()
ftp.retrbinary('RETR btransaction.loc', r.write)
rows = r.getvalue().split('\n')

months = {}   # (year, month)

current_dt = None
cday = None
ctime = None

for i, row in enumerate(rows):
    if your_code in row:
        dt = datetime.strptime(row[9:23], "%Y%m%d%H%M%S")

        if dt is None or dt.date() != cday:
            cday = dt.date()
            if (cday.year, cday.month) not in months:
                months[(cday.year, cday.month)] = {}
            if cday.day not in months[(cday.year, cday.month)]:
                months[(cday.year, cday.month)][cday.day] = []
            print '\n[{}]'.format(str(cday))

        if ctime is None or ctime != dt.time():
            current_dt = dt
            ctime = dt.time()
            months[(cday.year, cday.month)][cday.day].append(dt)
            print ctime
        else:
            continue

        if len(months[(cday.year, cday.month)][cday.day]) == 4:
            times = months[(cday.year, cday.month)][cday.day]
            hours = (times[3] - times[2]) + (times[1] - times[0])
            print 'Ore sgobbate: {}'.format(hours)

        if len(months[(cday.year, cday.month)][cday.day]) == 3 and cday == date.today():
            times = months[(cday.year, cday.month)][cday.day]
            hours = (datetime.now() - times[2]) + (times[1] - times[0])
            ape = "DAI CHE SI FA L'APE!!!!!!" if cday.isoweekday() == 5 else ''
            print 'Siamo a: {} ore ... {}'.format(hours, ape)


