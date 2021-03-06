#!/usr/bin/python

#  usage_remediation.py
#  This script perform sanity & remediation of cloudstack usage instances & volume records
# Author : Loic Lambiel @ exoscale


import MySQLdb
import argparse
import logging
import logging.handlers
import time
from datetime import datetime, timedelta
try:
    from raven import Client
except ImportError:
    Client = None
    pass

logfile = "/var/log/usage_remediation.log"
logging.basicConfig(format='%(asctime)s %(pathname)s %(levelname)s:%(message)s', level=logging.DEBUG, filename=logfile)


def main():
    parser = argparse.ArgumentParser(description='This script perform sanity & remediation of cloudstack usage records')
    parser.add_argument('-version', action='version', version='%(prog)s 1.0, Loic Lambiel exoscale')
    parser.add_argument('-dbhost', help='MYSQL host', required=True, type=str, dest='dbhost')
    parser.add_argument('-dbuser', help='DB username', required=True, type=str, dest='user')
    parser.add_argument('-dbpasswd', help='DB user password', required=True, type=str, dest='pwd')
    parser.add_argument('-simulate', help='Log events but do not remediate records', required=False, action='store_true',  dest='simulate')
    if Client is not None:
        parser.add_argument('-sentryapikey', help='Sentry API key', required=False, type=str, dest='sentryapikey')
    args = vars(parser.parse_args())
    return args


def remediatevolumes():

        logging.info('Starting volumes remediation pass')
        currentdatetime = time.strftime("%Y-%m-%d %H:%M:%S")
        querygetusagevolumes = "SELECT id FROM cloud_usage.usage_volume where deleted is null;"
        con = MySQLdb.connect(dbhost, user, pwd)
        con.autocommit(True)
        cursor = con.cursor()
        cursor.execute(querygetusagevolumes)
        rows = cursor.fetchall()
        usagevolumeid = []
        for row in rows:
            usagevolumeid.append(row[0])
        for id in usagevolumeid:
            querygetvolume = "SELECT removed FROM cloud.volumes where id like '%s';" % id
            cursor.execute(querygetvolume)
            rows = cursor.fetchall()
            for row in rows:
                csremoveddate = row[0]
                if csremoveddate is not None:
                    # prevent removal of unprocessed hourly usage
                    if (datetime.now() - csremoveddate) > timedelta(hours=6):
                        logging.warning('volume id %s active in usage but deleted in cloudstack !', id)
                        # remediate volume
                        if simulate is False:
                            querysetvolumeremoved = "UPDATE cloud_usage.usage_volume SET deleted = '%s' where id like '%s'" % (currentdatetime, id)
                            cursor.execute(querysetvolumeremoved)
                            logging.warning('volume id %s has been remediated in usage', id)
                        else:
                            logging.warning('volume id %s has been remediated in usage', id)
        con.close()
        logging.info('Completed volumes remediation pass')
        remediateinstances()


def remediateinstances():

        logging.info('Starting instances remediation pass')
        currentdatetime = time.strftime("%Y-%m-%d %H:%M:%S")
        con = MySQLdb.connect(dbhost, user, pwd)
        con.autocommit(True)
        cursor = con.cursor()

        # Remediate removed instances
        querygetusageinstances = "SELECT vm_instance_id FROM cloud_usage.usage_vm_instance where end_date is null;"
        cursor.execute(querygetusageinstances)
        rows = cursor.fetchall()
        usageinstanceid = []
        for row in rows:
            usageinstanceid.append(row[0])
        for id in usageinstanceid:
            querygetinstance = "SELECT removed FROM cloud.vm_instance where id = '%s';" % id
            cursor.execute(querygetinstance)
            rows = cursor.fetchall()
            for row in rows:
                csremoveddate = row[0]
                if csremoveddate is not None:
                    # prevent removal of unprocessed hourly usage
                    if (datetime.now() - csremoveddate) > timedelta(hours=4):
                        logging.warning('instance id %s existing in usage but removed in cloudstack !', id)
                        # remediate instance
                        if simulate is False:
                            querysetinstanceremoved = "UPDATE cloud_usage.usage_vm_instance SET end_date = '%s' where vm_instance_id = '%s'" % (currentdatetime, id)
                            cursor.execute(querysetinstanceremoved)
                            logging.warning('instance id %s has been remediated in usage (removed)', id)
                        else:
                            logging.warning('instance id %s has been remediated in usage (removed) simulated', id)

        # Remediate stopped instances
        querygetusageinstances = "SELECT vm_instance_id FROM cloud_usage.usage_vm_instance where end_date is null and usage_type = '1';"
        cursor.execute(querygetusageinstances)
        rows = cursor.fetchall()
        usageinstanceid = []
        for row in rows:
            usageinstanceid.append(row[0])
        for id in usageinstanceid:
            querygetinstance = "SELECT state, update_time FROM cloud.vm_instance where id = '%s';" % id
            cursor.execute(querygetinstance)
            rows = cursor.fetchall()
            for row in rows:
                csstate = row[0]
                csupdatedate = row[1]
                if csstate == 'Stopped':
                    # prevent removal of unprocessed hourly usage
                    if (datetime.now() - csupdatedate) > timedelta(hours=4):
                        logging.warning('instance id %s state is running in usage but stopped in cloudstack !', id)
                        # remediate instance
                        if simulate is False:
                            querysetinstancestopped = "UPDATE cloud_usage.usage_vm_instance SET end_date = '%s' where vm_instance_id = '%s' and usage_type = '1'" % (csupdatedate, id)
                            cursor.execute(querysetinstancestopped)
                            logging.warning('instance id %s has been remediated in usage (stopped)', id)
                        else:
                            logging.warning('instance id %s has been remediated in usage (stopped) simulated', id)
        con.close()
        logging.info('Completed instances remediation pass')


if __name__ == "__main__":
    global dbhost, user, pwd, simulate
    args = main()
    dbhost = args['dbhost']
    user = args['user']
    pwd = args['pwd']
    simulate = args['simulate']

    try:
        volumes = remediatevolumes()
    except Exception:
        if args['sentryapikey'] is None:
            raise
        else:
            client = Client(dsn=args['sentryapikey'])
            client.captureException()
