from google.cloud import storage
import zipfile
from io import StringIO
from io import BytesIO
from zipfile import ZipFile
from zipfile import is_zipfile
import os  

new_bucket = 'your-new-bucket'
# bucket_name=event['bucket']
# blob_name=event['name']
client = storage.Client()

def move_delete_blob(event, context):
    """Deletes a blob from the bucket."""
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))
    bucket= client.get_bucket(event['bucket'])
    blob = bucket.get_blob(event['name'])
    zipbytes = BytesIO(blob.download_as_string())
    # newfile = blob.download_as_string()
    # Upload the resampled file to the other bucket
    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                bucket= client.get_bucket(new_bucket)
                newblob=bucket.blob(contentfilename)
                if os.path.isdir(contentfile):
                    app_input=os.chdir(contentfile)
                    filenames = [files  for files in os.listdir(app_input) if files.find(".csv") != -1 ]
                    for filename in filenames:
                        newblob.upload_from_string(filename)
                        print(filename)
                else:
                    newblob.upload_from_string(contentfile)
                    print(contentfile)
    # Delete old file
    blob.delete()
    print("Blob {} deleted.".format(blob))