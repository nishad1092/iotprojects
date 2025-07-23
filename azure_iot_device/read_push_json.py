import os
import asyncio
import uuid
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
import time
from datetime import datetime

messages_to_send = 1


async def main():

    # The connection string for a device should never be stored in code. For the sake of simplicity we're using an environment variable here.
    
    conn_str = 'AdGBU='

    # The client object is used to interact with your Azure IoT hub.
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)

    # Connect the client.
    await device_client.connect()

    async def send_test_message(i):

        print("sending message #" + str(i))
        
        file_name = '/home/nishad/IoT_Projects/azure_iot_device/data.json'
        blob_name = os.path.basename(file_name)
        await device_client.send_message(blob_name)

        print("done sending message #" + str(i))

    # send `messages_to_send` messages in parallel
    for i in range(1, messages_to_send + 1):

        await asyncio.gather(*[send_test_message(i)]) 
        time.sleep(1)

    print(datetime.now())

    # Finally, shut down the client
    await device_client.shutdown()


if __name__ == "__main__":
    asyncio.run(main())