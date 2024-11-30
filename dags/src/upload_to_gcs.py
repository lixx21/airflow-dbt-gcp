import os
from google.cloud import storage
import json

def upload_data(data, filename, bucket_name, credentials):

    print("Insert JSON local")
    #TODO: Iterate through list to get dictionary only
    with open(f"{filename}.json", "a") as write_file:
        for result in data:
            # print(result)
            json.dump(result, write_file) 
            write_file.write("\n")
            
    print("Upload JSON to GCS")
    storage_client = storage.Client.from_service_account_json(credentials)
    
    print(storage_client)
    BUCKET = storage_client.get_bucket(bucket_name)
    blob = BUCKET.blob(f'{filename}.json')

    with open(f'{filename}.json', 'rb') as json_file:
        blob.upload_from_file(json_file)
        
    os.remove(f"{filename}.json")
            
    return "Success upload JSON"