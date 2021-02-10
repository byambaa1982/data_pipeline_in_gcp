from google.cloud import storage
import pandas as pd
import datetime
from io import StringIO
from io import BytesIO
import os

project_id='your-project-id'
destination_bucket='your-dest-bucket'


def chunk_csv_file(event,destination_bucket):
# it is mandatory initialize the storage client
    client = storage.Client()
    #please change the file's URI
    filename=event['name']
    myurl='gs://car_datalake/'+str(filename)
    print('Uploaded file: {}'.format(event['name']))
    for i,chunk in enumerate(pd.read_csv(myurl, chunksize=100000)):
        print('chunk shape is {}'.format(chunk.shape))
        filename=('chunk{}.xlsx'.format(i))
        f = BytesIO()
        chunk.to_excel(f)
        f.seek(0)
        bucket=client.get_bucket('result_excel_files')
        newblob=blob(filename)
        newblob.upload_from_file(f, content_type='xlsx')
    return f'check the results in the logs'