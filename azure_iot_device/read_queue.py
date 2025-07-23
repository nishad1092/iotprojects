import base64
import json 

from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)

import os, uuid


# variable named AZURE_STORAGE_CONNECTION_STRING
#connect_str = "BlobEndpoint=https://iotstorage101.blob.core.windows.net/;QueueEndpoint=https://iotstorage101.queue.core.windows.net/;FileEndpoint=https://iotstorage101.file.core.windows.net/;TableEndpoint=https://iotstorage101.table.core.windows.net/;SharedAccessSignature=sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacuptfx&se=2021-08-02T17:16:33Z&st=2021-06-02T09:16:33Z&spr=https&sig=0HU%2BIOBVOyx5qci51fx5bvAZdmHiJ0RvAYWUfy2r5kE%3D"
connect_str = ""
q_name = 'iotqueue-01'

queue_client = QueueClient.from_connection_string(connect_str, q_name)


# Peek at the first message
messages = queue_client.peek_messages()

for peeked_message in messages:
    decoded_msg = base64.b64decode(peeked_message.content).decode('utf-8')
    decoded_queue_msg = json.loads(decoded_msg)
    filepath = decoded_queue_msg['data']['url']
    file_read_status = decoded_queue_msg['data']['api']
    print(filepath, file_read_status)
