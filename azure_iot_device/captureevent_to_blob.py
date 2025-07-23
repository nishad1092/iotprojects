from datetime import datetime
import os
import string
import json
import uuid
import avro.schema

from azure.storage.blob import ContainerClient, BlobClient, BlobServiceClient
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def processBlob2(filename):
    reader = DataFileReader(open(filename, 'rb'), DatumReader())
    dict = {}
    for reading in reader:
        parsed_json = json.loads(reading["Body"])
        if not 'id' in parsed_json:
            return
        if not parsed_json['id'] in dict:
            list = []
            dict[parsed_json['id']] = list
        else:
            list = dict[parsed_json['id']]
            list.append(parsed_json)
    reader.close()
    for device in dict.keys():
        global local_file_name
        local_file_name = os.getcwd() + '/' + 'Azure_Blob_Data' + '/' + str(device) + '.json'
        deviceFile = open(local_file_name, "a")
        for r in dict[device]:
            deviceFile.write(", ".join([str(r[x]) for x in r.keys()])+'\n')

def upload_to_blob():

    connect_str = "BlobEndpoint=https://iotstorage101.blob.core.windows.net/;QueueEndpoint=https://iotstorage101.queue.core.windows.net/;FileEndpoint=https://iotstorage101.file.core.windows.net/;TableEndpoint=https://iotstorage101.table.core.windows.net/;SharedAccessSignature=sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacuptfx&se=2021-08-02T17:16:33Z&st=2021-06-02T09:16:33Z&spr=https&sig=0HU%2BIOBVOyx5qci51fx5bvAZdmHiJ0RvAYWUfy2r5kE%3D"


    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    # # Create a unique name for the container
    # container_name = str(uuid.uuid4())

    # # Create the container
    # container_client = blob_service_client.create_container(container_name)
    
    blob_client = blob_service_client.get_blob_client(container="iotdata101", blob=local_file_name)

    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
    try:
        if os.path.exists(local_file_name):

            # Upload the created file
            with open(local_file_name, "rb") as data:
                blob_client.upload_blob(data)
    except Exception as e:
        print("No json file for {}".format(local_file_name))


def startProcessing():
    print('Processor started using path: ' + os.getcwd())
    # Create a blob container client.
    container = ContainerClient.from_connection_string("Bl", container_name="iotdata101")
    blob_list = container.list_blobs() # List all the blobs in the container.
    for blob in blob_list:
        # Content_length == 508 is an empty file, so process only content_length > 508 (skip empty files).        
        if blob.size > 500:
            print('Downloaded a non empty blob: ' + blob.name)
            # Create a blob client for the blob.
            blob_client = ContainerClient.get_blob_client(container, blob=blob.name)
            # Construct a file name based on the blob name.
            cleanName = str.replace(blob.name, '/', '_')
            cleanName = os.getcwd() + '/' + 'Azure_Blob_Data' + '/' + cleanName 
            with open(cleanName, "wb+") as my_file: # Open the file to write. Create it if it doesn't exist. 
                my_file.write(blob_client.download_blob().readall()) # Write blob contents into the file.
            processBlob2(cleanName) # Convert the file into a CSV file.
            upload_to_blob()
            os.remove(cleanName) # Remove the original downloaded file.
            # Delete the blob from the container after it's read.
            #container.delete_blob(blob.name)
            print(datetime.now())

startProcessing()