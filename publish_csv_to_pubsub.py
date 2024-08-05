import time
import argparse
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from configparser import ConfigParser

def get_callback(future, data):
    def callback(future):
        try:
            print(future.result())
            futures.pop(data)
        except Exception as e:
            print(f"Error: {e} for {data}")
    return callback

def remove_quotes_for_int_values(obj):
    if isinstance(obj, list):
        return [remove_quotes_for_int_values(el) for el in obj]
    elif isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if isinstance(value, (dict, list)):
                result[key] = remove_quotes_for_int_values(value)
            else:
                try:
                    if value == '' and key != 'CANCELLATION_REASON':
                        value = 0
                    result[key] = int(value)
                except ValueError:
                    result[key] = value
        return result
    else:
        return obj

def publish(filepath):
    with open(filepath, encoding="utf8") as openfileobject:
        for i, line in enumerate(openfileobject):
            if i == 0:
                global keys
                keys = line.strip().split(',')
            else:
                data_dict = dict(zip(keys, line.strip().split(',')))
                data = str(remove_quotes_for_int_values(data_dict))
                futures[data] = None
                future = publisher.publish(topic_path, data.encode("utf-8"))
                futures[data] = future
                future.add_done_callback(get_callback(future, data))

    # Wait for all futures to complete
    while futures:
        time.sleep(1)

    print("Publish messages with error handler to " + topic_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config_path', required=True,
        help='Config path from where config will be read.')
    args = parser.parse_args()

    config = ConfigParser()
    config.read(args.config_path)

    credentials = service_account.Credentials.from_service_account_file(
        config.get('gcp', 'credential_path'))

    project_id = config.get('gcp', 'project_id')
    topic_id = config.get('gcp', 'topic_id')

    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_path = publisher.topic_path(project_id, topic_id)
    futures = {}

    publish(config.get('gcp', 'file_path'))
