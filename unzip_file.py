from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile
import io

from google.cloud import storage
import zipfile
from io import StringIO
from io import BytesIO
from zipfile import ZipFile
from zipfile import is_zipfile


new_bucket = 'car_datalake'
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
                newblob.upload_from_string(contentfile)
    # Delete old file
    blob.delete()
    print("Blob {} deleted.".format(blob))