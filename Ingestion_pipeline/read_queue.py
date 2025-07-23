import base64
import json 
import yaml

from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)

import os, uuid

#from yaml import loader


# variable named AZURE_STORAGE_CONNECTION_STRING
#connect_str = """

# Read the configuration from yaml

def read_config():
        with open(r'./config.yaml') as config_file:
                config_yaml = yaml.load(config_file, Loader=yaml.FullLoader)
                print(config_yaml)
                print(config_file)


                
        # connect_str = config_yaml["CONNECT_STRING"]
        # q_name = config_yaml["QUEUE_NAME"]

        # queue_client = QueueClient.from_connection_string(connect_str, q_name)


        # # Peek at the first message
        # messages = queue_client.peek_messages()

        # for peeked_message in messages:
        #         decoded_msg = base64.b64decode(peeked_message.content).decode('utf-8')
        #         decoded_queue_msg = json.loads(decoded_msg)
        #         filepath = decoded_queue_msg['data']['url']
        #         file_read_status = decoded_queue_msg['data']['api']
        #         print(filepath, file_read_status)


if __name__ == '__main__':
        read_config()
