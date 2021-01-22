from google.cloud import storage
import pandas as pd
import datetime
from io import StringIO
from io import BytesIO

project_id=['YOUR-PROJECT-ID']
destination_bucket=['YOUR-DESTINATION-BUCKET']


def chunk_csv_file(event,destination_bucket):
    # it is mandatory initialize the storage client
    client = storage.Client()
    #please change the file's URI
    filename=event['name']
    myurl='gs://car_datalake/'+str(filename)
    temp = pd.read_csv(myurl, encoding='utf-8')
    print ('Total shape: {}'.format(temp.shape))
    print('Uploaded file: {}'.format(event['name']))
    my_time=str(str(datetime.datetime.today())[11:19])
    new_file=str('created_at_')+str(my_time)+str('.xlsx')
    print('new file: {}'.format(new_file))
    print('destination Bucket: {}'.format(destination_bucket))
    df=temp
    df = df.applymap(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else x)
    n=1000000
    m=round(df.shape[0]/n)
    for i in range(0,m+1):
        f = BytesIO()
        sub_df=df.iloc[i*n:(i+1)*n]
        sub_df.to_excel(f)
        print(sub_df.shape)
        f.seek(0)
        if i==0:
            client.get_bucket('result_excel_files').blob('file_1.xlsx').upload_from_file(f, content_type='xlsx')
        elif i==1:
            client.get_bucket('result_excel_files').blob('file_2.xlsx').upload_from_file(f, content_type='xlsx')
        elif i==2:
            client.get_bucket('result_excel_files').blob('file_3.xlsx').upload_from_file(f, content_type='xlsx')
        elif i==3:
            client.get_bucket('result_excel_files').blob('file_4.xlsx').upload_from_file(f, content_type='xlsx')
        elif i==4:
            client.get_bucket('result_excel_files').blob('file_5.xlsx').upload_from_file(f, content_type='xlsx')
        elif i==5:
            client.get_bucket('result_excel_files').blob('file_6.xlsx').upload_from_file(f, content_type='xlsx')
        else:
            client.get_bucket('result_excel_files').blob('file_7.xlsx').upload_from_file(f, content_type='xlsx')
    return f'check the results in the logs'