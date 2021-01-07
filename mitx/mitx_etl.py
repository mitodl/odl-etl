#!/usr/bin/env python

"""Script to run SQL queries on mitx residential and upload them to an S3 bucket."""

import json
import os
import subprocess  # noqa:S404
import sys
import tarfile
from datetime import datetime

import requests
from envbash import load_envbash
from logbook import Logger, RotatingFileHandler

datetime = datetime.now()
date_suffix = datetime.strftime('%Y%m%d')
dir_path = os.path.dirname(os.path.realpath(__file__))

# Read settings_file and load env
try:
    settings = json.load(open(os.path.join(dir_path, './settings.json')))  # noqa: WPS515
    load_envbash('/edx/app/edxapp/edxapp_env')
except IOError:
    sys.exit('[-] Failed to read settings file')

# Configure logbook logging
logger = RotatingFileHandler(
    settings['Logs']['logfile'],
    max_size=int(settings['Logs']['max_size']),
    backup_count=int(settings['Logs']['backup_count']),
    level=int(settings['Logs']['level']),
)
logger.push_application()
logger = Logger('mitx_etl')

# Set some needed variables
course_ids = []
exported_courses_folder = settings['Paths']['courses'] + date_suffix + '/'
daily_folder = settings['Paths']['csv_folder'] + date_suffix + '/'


def set_environment_variables():
    """Set some of the read settings as environment variables."""
    os.environ['AWS_ACCESS_KEY_ID'] = settings['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = settings['AWS']['AWS_SECRET_ACCESS_KEY']


def verify_and_create_required_folders(courses):
    """Check whether the folder that will contain csv query files exists.

    Args:
      csv_folder (str): The path of the csv folder.

    Returns:
      If folder exists return None, and if not, logs error and exit.
    """
    if not os.path.exists(daily_folder):
        os.makedirs(daily_folder)
        logger.info('csv folder(s) created')

    if not os.path.exists(exported_courses_folder):
        os.makedirs(exported_courses_folder)
        logger.info('exported_courses_folder created')


def export_all_courses(exported_courses_folder):
    """Export all courses into specified folder.

    Args:
      exported_courses_folder (str): The path of folder to export courses to.
    """
    try:
        course_list = subprocess.Popen(
            [
                '/edx/app/edxapp/venvs/edxapp/bin/python',
                '/edx/app/edxapp/edx-platform/manage.py',
                'cms',
                '--settings',
                'production',
                'dump_course_ids',
            ],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = course_list.communicate()
        if out:
            for course_id in out.splitlines():
                export_course = subprocess.Popen(
                    [
                        '/edx/app/edxapp/venvs/edxapp/bin/python',
                        '/edx/app/edxapp/edx-platform/manage.py',
                        'cms',
                        '--settings',
                        'production',
                        'export_olx', course_id, '--output',
                        '{0}/{1}.tar.gz'.format(
                            exported_courses_folder,
                            course_id,
                        ),
                    ],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                out, err = export_course.communicate()
    except ValueError:
        logger.exception("Failed to dump_course_ids")
        sys.exit(1)


def tar_exported_courses(exported_courses_folder):
    """Tar exported course folders and store them in daily_folder.

    Args:
      exported_courses_folder (str): The path of folder to export courses to.
    """
    try:
        with tarfile.open(daily_folder + 'exported_courses_' + date_suffix + '.tar.gz', 'w:gz') as tar:
            tar.add(exported_courses_folder, arcname=os.path.sep)
    except tarfile.TarError as err:
        logger.exception(
            'The following error was encountered when compressing exported courses: ',
            err,
        )


def sync_to_s3(daily_folder, s3_bucket_name):
    """Sync local files to specified S3 bucket.

    Args:
      daily_folder (str): folder containing msql query results.
      s3_bucket_name (str): s3 bucket name
    """
    try:
        cmd_output = subprocess.run(
            [
                'aws',
                's3',
                'sync',
                daily_folder,
                's3://' + s3_bucket_name + '/' + date_suffix,
            ],
            check=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except SyntaxError as err:
        logger.exception(
            'Failed to run sync command. Check if awscli is installed'
        )
        notify_slack_channel('Failed to run sync command: `{0}`'.format(err))
        sys.exit('[-] Failed to run sync command. Check if awscli is installed')
    except subprocess.SubprocessError as err:
        logger.exception('Failed to sync local files to s3 bucket')
        notify_slack_channel('Sync failed: `{0}`'.format(err))
        sys.exit('[-] Failed to sync daily_folder to s3 bucket')
    logger.info('S3 sync successfully ran: {0}', cmd_output)
    notify_slack_channel('Sync succeeded: `str({0}`)'.format(cmd_output))
    logger.info('Syncing complete')


def notify_slack_channel(slack_message):
    """Send notification to Slack Channel.

    Args:
      slack_message (str): message to send to slack
    """
    try:
        requests.post(
            settings['Slack']['webhook_url'],
            json={
                'text': slack_message,
                'username': settings['Slack']['bot_username'],
                'icon_emoji': settings['Slack']['bot_emoji'],
            },
        )
    except (requests.exceptions.RequestException, NameError) as err:
        logger.warn(
            'Failed to notify slack channel with following error: {}',
            err,
        )


def run_healthcheck(url):
    """Ping healthcheck endpoint.

    Args:
      url (str): healtcheck endpoint url
    """
    try:
        requests.get(url)
    except requests.exceptions.RequestException as err:
        logger.exception(
            'Failed to ping healthcheck with following error: ', err,
        )
        sys.exit(1)


def main():
    set_environment_variables()
    verify_and_create_required_folders(
        settings['Paths']['courses'],
    )
    export_all_courses(exported_courses_folder)
    tar_exported_courses(exported_courses_folder)
    sync_to_s3(daily_folder, settings['S3Bucket']['bucket'])
    run_healthcheck(settings['Healthchecks']['url'])


if __name__ == '__main__':
    main()
