from google.cloud import storage
import pandas as pd
import datetime
from io import StringIO
from io import BytesIO
import os
from pyexcelerate import Workbook
import time

project_id='your-project-id'
destination_bucket='your-dest-bucket'


def chunk_csv_file(event,destination_bucket):
# it is mandatory initialize the storage client
    client = storage.Client()
    #please change the file's URI
    fname=event['name']
    myurl='gs://car_datalake/'+str(fname)
    print('Uploaded file: {}'.format(event['name']))
    for i,chunk in enumerate(pd.read_csv(myurl, chunksize=1000000)):
        print('chunk shape is {}'.format(chunk.shape))
        print('loop is {}'.format(i))
        print('chunk shape is {}'.format(chunk.shape))
        try:
            # start = datetime.datetime.now()[11:19]
            filename=('chunk{}.xlsx'.format(i))
            print(filename)
            f = BytesIO()
            # chunk.to_excel(f)
            start = time.time()
            wb = Workbook()
            wb.new_sheet("car data", data=chunk)
            wb.save(f)
            end = time.time()
            print("Read csv and to excel: ",(end-start),"sec")
            f.seek(0)
            bucket=client.get_bucket('result_excel_files')
            newblob=bucket.blob(filename)
            newblob.upload_from_file(f, content_type='xlsx')
        except:
            print('something wrong: {}'.format(i))
    return f'check the results in the logs'