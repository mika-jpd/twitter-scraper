import os
import boto3
import configparser
from tqdm import tqdm
import datetime


def upload_to_s3(bucket_name, filepath, folder=None):
    if os.stat(filepath).st_size == 0:
        return
    s3_client = boto3.client('s3', region_name=os.environ["AWS_REGION"])
    location = {'LocationConstraint': os.environ["AWS_REGION"]}
    s3_obj = filepath[filepath.rfind(os.sep) + 1:]
    if folder:
        response = s3_client.upload_file(filepath, bucket_name, '{}/{}'.format(folder, s3_obj))
    else:
        response = s3_client.upload_file(filepath, bucket_name, s3_obj)

    return response


def save_files_to_s3_(config_path: str = "/Users/mikad/MEOMcGill/meo-crawlers/twitter_api_client/config.ini",
                      start_date: str | None = None,
                      end_date: str | None = None):
    # config
    config = configparser.ConfigParser()
    config.read(config_path)

    AWS_ACCESS_KEY_ID = config['s3authentication']['AWS_ACCESS_KEY_ID']
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = config['s3authentication']['AWS_SECRET_ACCESS_KEY']
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    AWS_REGION = config['s3authentication']['AWS_REGION']
    os.environ["AWS_REGION"] = AWS_REGION

    if not start_date or not end_date:
        try:
            # dates
            start_date = config['date']['start']
            end_date = config['date']['end']
        except KeyError:
            start_date = datetime.datetime.today() - datetime.timedelta(days=3)
            end_date = datetime.datetime.today() - datetime.timedelta(days=2)

            start_date = start_date.strftime('%Y-%m-%d')
            end_date = end_date.strftime('%Y-%m-%d')

    try:
        path_output = config['paths']['output']
    except KeyError:
        path_output = '../../output'
    try:
        data_dir = config['paths']['data']
    except KeyError:
        data_dir = os.path.join('data', f'twitter_{start_date}_{end_date}')

    path_output_data = os.path.join(path_output, data_dir)
    path_logs = os.path.join(path_output, 'logs')
    path_log_for_this_run = os.path.join(path_logs, f'logs_twitter_{start_date}_{end_date}.txt')
    print(f'Uploading files from {path_output_data}')
    # get today's tweets
    for file in tqdm(os.listdir(path_output_data)):
        file_path = os.path.join(path_output_data, file)
        response = upload_to_s3(config["s3paths"]["bucket"],
                                filepath=file_path,
                                folder=config["s3paths"]["folder"])


def save_files_to_s3(config_path: str, output_folder_path: str):
    # config
    config = configparser.ConfigParser()
    config.read(config_path)

    s3_path = config["s3paths"]["folder"]

    print(f'Uploading files from {output_folder_path}')
    # get today's tweets
    for file in tqdm(os.listdir(output_folder_path)):
        file_path = os.path.join(output_folder_path, file)
        response = upload_to_s3(config["s3paths"]["bucket"],
                                filepath=file_path,
                                folder=s3_path)
