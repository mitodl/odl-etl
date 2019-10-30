#!/usr/bin/env python

"""
Script to run SQL queries on mitx residential and upload them to an S3 bucket.
"""

import csv
import json
import os
import subprocess
import sys
import tarfile
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
date_suffix = datetime.strftime('%Y%m%d')
dir_path = os.path.dirname(os.path.realpath(__file__))

# Read settings_file
try:
    settings = json.load(open(os.path.join(dir_path, './settings.json')))
except IOError:
    sys.exit("[-] Failed to read settings file")

# Configure logbook logging
logger = RotatingFileHandler(settings['Logs']['logfile'],
                             max_size=int(settings['Logs']['max_size']),
                             backup_count=int(settings['Logs']['backup_count']),
                             level=int(settings['Logs']['level']))
logger.push_application()
logger = Logger('mitx_etl')

# Set some needed variables
mysql_creds_user = settings['MySQL']['user']
mysql_creds_pass = settings['MySQL']['pass']
mysql_host = settings['MySQL']['host']
mysql_db = settings['MySQL']['db']
mongodb_host = settings['Mongodb']['host']
mongodb_port = settings['Mongodb']['port']
mongodb_user = settings['Mongodb']['user']
mongodb_password = settings['Mongodb']['password']
forum_db = settings['Mongodb']['forum_db']
course_ids = []
exported_courses_folder = settings['Paths']['courses'] + date_suffix + '/'
forums_data_folder = settings['Paths']['forum_data'] + date_suffix + '/'
daily_folder = settings['Paths']['csv_folder'] + date_suffix + '/'

# List of db queries
query_dict = {
    'users_query': {'command': 'select auth_user.id, auth_user.username, auth_user.first_name, auth_user.last_name, auth_user.email, auth_user.is_staff, auth_user.is_active, auth_user.is_superuser, auth_user.last_login, auth_user.date_joined from auth_user inner join student_courseenrollment on student_courseenrollment.user_id = auth_user.id and student_courseenrollment.course_id = :course_id', 'fieldnames':  ['id', 'username', 'first_name', 'last_name', 'email', 'is_staff', 'is_active', 'is_superuser', 'last_login', 'date_joined']},
    'studentmodule_query': {'command': 'select id, module_type, module_id, student_id, state, grade, created, modified, max_grade, done, course_id from courseware_studentmodule where course_id= :course_id', 'fieldnames': ['id', 'module_type', 'module_id', 'student_id', 'state', 'grade', 'created', 'modified', 'max_grade', 'done', 'course_id']},
    'enrollment_query': {'command': 'select id, user_id, course_id, created, is_active, mode  from student_courseenrollment where course_id= :course_id', 'fieldnames': ['id', 'user_id', 'course_id', 'created', 'is_active', 'mode']},
    'role_query': {'command': 'select id,user_id,org,course_id,role from student_courseaccessrole where course_id= :course_id', 'fieldnames': ['id', 'user_id', 'org', 'course_id', 'role']}
}


def set_environment_variables():
    """
    Set some of the read settings as environment variables.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = settings['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = settings['AWS']['AWS_SECRET_ACCESS_KEY']


def verify_and_create_required_folders(csv_folder, courses, forums_data_folder):
    """
    Check whether the folder that will contain csv query files exists

    Args:
      csv_folder (str): The path of the csv folder.

    Returns:
      If folder exists return None, and if not, logs error and exit.
    """
    if not os.path.exists(daily_folder):
        os.makedirs(daily_folder)
        logger.info("csv folder(s) created")

    if not os.path.exists(exported_courses_folder):
        os.makedirs(exported_courses_folder)
        logger.info("exported_courses_folder created")
     
    if not os.path.exists(forums_data_folder):
        os.makedirs(forums_data_folder)
        logger.info("forums_data_folder created")

    if not os.path.exists(forums_data_folder):
        os.makedirs(settings['Paths']['forums_data_folder'] + date_suffix + '/')
        logger.info("forums_data_folder created")


def export_all_courses(exported_courses_folder):
    """
    Export all courses into specified folder

    Args:
      exported_courses_folder (str): The path of folder to export courses to.

    """
    try:
        course_list = subprocess.Popen(
            ['/edx/bin/python.edxapp',
             '/edx/app/edxapp/edx-platform/manage.py',
             'cms', '--settings', 'production',
             'dump_course_ids'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = course_list.communicate()
        for course_id in out.splitlines():
            export_course = subprocess.Popen(
                ['/edx/bin/python.edxapp',
                 '/edx/app/edxapp/edx-platform/manage.py',
                 'cms', '--settings', 'production',
                 'export_olx', course_id.encode('utf8'), '--output',
                 '{0}/{1}.tar.gz'.format(exported_courses_folder,
                                         course_id.encode('utf8'))],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = export_course.communicate()
    except ValueError as err:
            logger.exception(
                "The following error was encountered when exporting courses: ",
                err)


def tar_exported_courses(exported_courses_folder):
    """
    Tar exported course folders and store them in daily_folder

    Args:
      exported_courses_folder (str): The path of folder to export courses to.
    """
    try:
        with tarfile.open(daily_folder + 'exported_courses_' + date_suffix + '.tar.gz', 'w:gz') as tar:
            tar.add(exported_courses_folder, arcname=os.path.sep)
    except tarfile.TarError as err:
        logger.exception("The following error was encountered when compressing exported courses: ", err)


def get_list_of_staff():
    pass


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


def add_csv_header():
    """
    Create csv files and add header to each based on
    fieldnames in query_dict
    """
    for key, value in query_dict.items():
        with open(daily_folder + str(key) + '.csv', 'w+', encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=value['fieldnames'])
            writer.writeheader()


def mysql_query(course_ids):
    engine = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'
                           .format(mysql_creds_user, mysql_creds_pass, mysql_host, mysql_db))
    connection = engine.connect()
    for course_id in course_ids:
        for key, value in query_dict.items():
            query_text = text(value['command'])
            query = connection.execute(query_text, course_id=course_id.decode('utf8'))
            write_csv(query, key)


def write_csv(query, key):
    with open(daily_folder + str(key) + '.csv', 'a', encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in query:
            writer.writerow(row)


def get_forums_data(mongodb_host, mongodb_port, mongodb_password, mongodb_user,
                    forum_db, forums_data_folder):
    dump_forums_data = subprocess.Popen(['/usr/bin/mongodump', '--host',
                                         mongodb_host, '--port',
                                         mongodb_port, '--password',
                                         mongodb_password, '--username',
                                         mongodb_user,
                                         '--authenticationDatabase',
                                         'admin', '--db', forum_db,
                                         '--out', forums_data_folder],
                                        stdout=subprocess.PIPE)
    logger.info('Forums data dumped')


def sync_to_s3(daily_folder, forums_data_folder, s3_bucket_name):
    """
    Sync local files to specified S3 bucket

    Args:
      daily_folder (str): folder containing msql query results.
      s3_bucket_name (str): s3 bucket name
    """
    for folder in [daily_folder, forums_data_folder]:
        try:
            cmd_output = subprocess.run(["aws", "s3", "sync", folder,
                                        "s3://" + s3_bucket_name + '/' + date_suffix],
                                        check=True, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
        except SyntaxError as err:
            logger.exception("Failed to run sync command. Check if awscli is installed")
            notify_slack_channel("Failed to run sync command: `{}`".format(err))
            sys.exit("[-] Failed to run sync command. Check if awscli is installed")
        except subprocess.SubprocessError as err:
            logger.exception("Failed to sync local files to s3 bucket")
            notify_slack_channel("Sync failed: `{}`".format(err))
            sys.exit("[-] Failed to sync daily_folder to s3 bucket")
    logger.info("S3 sync successfully ran: {}", cmd_output)
    notify_slack_channel("Sync succeeded: `str({}`)".format(cmd_output))
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


def run_healthcheck(url):
    """
    Ping healthcheck endpoint

    Args:
      url (str): healtcheck endpoint url
    """
    try:
        requests.get(url)
    except requests.exceptions.RequestException as err:
        logger.exception("Failed to ping healthcheck with following error: ", err)
        sys.exit(1)


def main():
    set_environment_variables()
    verify_and_create_required_folders(settings['Paths']['csv_folder'],
                                       settings['Paths']['courses'],
                                       settings['Paths']['forum_data'])
    export_all_courses(exported_courses_folder)
    tar_exported_courses(exported_courses_folder)
    add_csv_header()
    get_course_ids()
    mysql_query(course_ids)
    get_forums_data(mongodb_host, mongodb_port, mongodb_password, mongodb_user,
                   forum_db, forums_data_folder)
    sync_to_s3(daily_folder, forums_data_folder, settings['S3Bucket']['bucket'])
    run_healthcheck(settings['Healthchecks']['url'])


if __name__ == "__main__":
    main()
