from google.cloud import storage
import pandas as pd
import datetime
from io import StringIO
from io import BytesIO

project_id='steven-datapipeline'
destination_bucket='result_excel_files'
my_time=str(str(datetime.datetime.today())[10:19])


def chunk_csv_file(event, my_time):
    # it is mandatory initialize the storage client
    client = storage.Client()
    #please change the file's URI
    filename=event['name']
    myurl='gs://car_datalake/'+str(filename)
    temp = pd.read_csv(myurl, encoding='utf-8')
    print ('Total shape: {}'.format(temp.shape))
    print('File: {}'.format(event['name']))
    df=temp
    f = BytesIO()
    df.to_excel(f)
    f.seek(0)
    # new_file='created_at_'+my_time+'.csv'
    client.get_bucket('result_excel_files').blob('new_file.xlsx').upload_from_file(f, content_type='xlsx')
    return f'check the results in the logs'