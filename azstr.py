from azure.storage.blob import BlobServiceClient, BlobClient
import os

connstr = connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

blobsrvclient = BlobServiceClient.from_connection_string(connstr)

print(blobsrvclient.get_account_information())

# print(blobsrvclient.get_service_stats())
for cntitem in blobsrvclient.list_containers():
    cntclient = blobsrvclient.get_container_client(cntitem.name) # get a container client object
    print(cntitem.name+'\n\n')

    for blbitem in cntclient.list_blobs(): #list all objects in the container
        print(blbitem.name)


