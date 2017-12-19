#!/usr/bin/env python

"""
Script to run SQL queries on mitx residential and upload them to an S3 bucket.
"""

import csv
import json
import os
import subprocess
import sys
from datetime import datetime

try:
    import requests
    from logbook import Logger, RotatingFileHandler
    from sqlalchemy import create_engine
    from sqlalchemy.sql import text
except ImportError as err:
    print("Failed to import module: ", err)
    sys.exit("Make sure to install logbook, requests and sqlalchemy")

datetime = datetime.now()
date_suffix = datetime.strftime('%m%d%Y')

# Read settings_file
try:
    settings = json.load(open('settings.json'))
except IOError:
    sys.exit("[-] Failed to read settings file")

# Configure logbook logging
logger = RotatingFileHandler(settings['Logs']['logfile'],
                             max_size=int(settings['Logs']['max_size']),
                             backup_count=int(settings['Logs']['backup_count']),
                             level=int(settings['Logs']['level']))
logger.push_application()
logger = Logger(__name__)

# Set some needed variables
mysql_creds_user = settings['MySQL']['user']
mysql_creds_pass = settings['MySQL']['pass']
mysql_host = settings['MySQL']['host']
mysql_db = settings['MySQL']['db']
course_ids = []
daily_folder = settings['Paths']['csv_folder'] + date_suffix + '/'

# List of db queries
query_dict = {
    'users_query': 'select auth_user.* from auth_user inner join student_courseenrollment on student_courseenrollment.user_id = auth_user.id and student_courseenrollment.course_id = :course_id',
    'studentmodule_query': 'select * from courseware_studentmodule where course_id= :course_id',
    'enrollment_query': 'select * from student_courseenrollment where course_id= :course_id'
}


def set_environment_variables():
    """
    Set some of the read settings as environment variables.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = settings['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = settings['AWS']['AWS_SECRET_ACCESS_KEY']


def verify_and_create_daily_csv_folder(csv_folder):
    """
    Check whether the folder that will contain csv query files exists

    Args:
      csv_folder (str): The path of the csv folder.

    Returns:
      If folder exists return None, and if not, logs error and exit.
    """
    if not os.path.exists(daily_folder):
        os.makedirs(settings['Paths']['csv_folder'] + date_suffix + '/')
        logger.info("csv folder(s) created")


def get_course_ids():
    """
    Get a list of course ids that is necessary for the rest of the
    functions to work.
    """
    global course_ids
    dump_course_ids = subprocess.Popen(['/edx/bin/python.edxapp',
                                        '/edx/app/edxapp/edx-platform/manage.py',
                                        'lms', '--settings', 'aws',
                                        'dump_course_ids'], stdout=subprocess.PIPE)
    course_ids = dump_course_ids.communicate()[0].split()
    return course_ids


def export_course(course_ids):
    for course_id in course_ids:
        with subprocess.Popen(['/edx/bin/python.edxapp',
                               '/edx/app/edxapp/edx-platform/manage.py',
                               'lms', '--settings', 'aws',
                               'export_course', course_id, course_id + b'.tar.gz'], stdout=subprocess.PIPE) as proc:
            logger.info(proc.stdout.read())


def get_list_of_staff():
    pass


def mysql_query(course_ids):
    engine = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'
                           .format(mysql_creds_user, mysql_creds_pass, mysql_host, mysql_db))
    connection = engine.connect()
    for course_id in course_ids:
        for key, value in query_dict.items():
            query_text = text(value)
            query = connection.execute(query_text, course_id=course_id.decode('utf8'))
            write_csv(query, key)


def write_csv(query, key):
    with open(daily_folder + str(key) + '.csv', 'a+') as f:
        writer = csv.writer(f)
        for row in query:
            writer.writerow(row)
    f.close()


def sync_to_s3(daily_folder, s3_bucket_name):
    """
    Sync local files to specified S3 bucket

    Args:
      daily_folder (str): folder containing msql query results.
      s3_bucket_name (str): s3 bucket name
    """
    try:
        cmd_output = subprocess.run(["aws", "s3", "sync", daily_folder,
                                    "s3://" + s3_bucket_name + '/' + date_suffix],
                                    check=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
    except SyntaxError as err:
        logger.exception("Failed to run sync command. Check if awscli is installed")
        notify_slack_channel(f"Failed to run sync command: `{err}`")
        sys.exit("[-] Failed to run sync command. Check if awscli is installed")
    except subprocess.SubprocessError as err:
        logger.exception("Failed to sync local files to s3 bucket")
        notify_slack_channel(f"Sync failed: `{err}`")
        sys.exit("[-] Failed to sync daily_folder to s3 bucket")
    logger.info("S3 sync successfully ran: {}", cmd_output)
    notify_slack_channel(f"Sync succeeded: `str({cmd_output}`)")
    logger.info("Syncing complete")


def notify_slack_channel(slack_message):
    """
    Send notification to Slack Channel

    Args:
      slack_message (str): message to send to slack
    """
    try:
        requests.post(
            settings['Slack']['webhook_url'],
            json={
                "text": slack_message,
                "username": settings['Slack']['bot_username'],
                "icon_emoji": settings['Slack']['bot_emoji'], })
    except (requests.exceptions.RequestException, NameError) as err:
        logger.warn("Failed to notify slack channel with following error: {}", err)


def main():
    set_environment_variables()
    verify_and_create_daily_csv_folder(settings['Paths']['csv_folder'])
    get_course_ids()
    export_course(course_ids)
    mysql_query(course_ids)
    sync_to_s3(daily_folder, settings['S3Bucket']['bucket'])


if __name__ == "__main__":
    main()
