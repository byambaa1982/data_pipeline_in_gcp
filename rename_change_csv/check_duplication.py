from google.cloud import storage
import pandas as pd
from datetime import datetime
from io import StringIO
from io import BytesIO
import os

os.environ["GCLOUD_PROJECT"] = "twittersheet-275317"
project_id='twittersheet-275317'
destination_bucket='getting-termites-tweet'

def get_cols_list():
  os.environ["GCLOUD_PROJECT"] = "my-bigquery-lab-286400"
  client=storage.Client()
  bucket = client.bucket('getting-termites-tweet')
  cols=[]
  for blob in bucket.list_blobs():
    print(blob.name)
    filename=blob.name
    try:   
      myurl='gs://getting-termites-tweet/'+str(filename)
      df=pd.read_csv(myurl, nrows=1)
      cols.append(df.columns[4])
      print(df.columns[4])
    except:
      continue
  return cols

def check_and_trans(event,destination_bucket):
  client = storage.Client()
  #please change the file's URI
  fname=event['name']
  myurl='gs://staging.twittersheet-275317.appspot.com/'+str(fname)
  print('Uploaded file: {}'.format(event['name']))
  df=pd.read_csv(myurl)
  print('url is {}'.format(myurl))
  print('chunk shape is {}'.format(df.shape))
  date_until=datetime.today().strftime('%Y-%m-%d')
  filename=('test_v4_{}.csv'.format(date_until))
  cols=get_cols_list()
  if df.columns[4] not in cols:
    f = StringIO()
    df.to_csv(f, index=False)
    f.seek(0)
    client=storage.Client()
    bucket=client.get_bucket('getting-termites-tweet')
    newblob=bucket.blob("temp_file.csv")
    newblob.upload_from_string(f.read(), content_type='text/csv')
    print('uploaded storage')
  else:
    pass
  return f'check the results in the logs'