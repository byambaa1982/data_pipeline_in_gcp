from google.cloud import storage
import pandas as pd
from datetime import datetime
from io import StringIO
from io import BytesIO
import os

os.environ["GCLOUD_PROJECT"] = "juva-brands"
project_id='juva-brands'
destination_bucket='dump-amazon-brand-analytics-search-terms-12345'

def get_cols_list():
  os.environ["GCLOUD_PROJECT"] = "juva-brands"
  client=storage.Client()
  bucket = client.bucket('processing-amazon-brand-analytics-search-terms-12345')
  cols=[]
  for blob in bucket.list_blobs():
    print(blob.name)
    filename=blob.name
    try:   
      myurl='gs://processing-amazon-brand-analytics-search-terms-12345/'+str(filename)
      df=pd.read_csv(myurl, nrows=1)
      cols.append(df.columns[4])
      print(df.columns[4])
    except:
      continue
  return cols

def check_and_trans(event,destination_bucket):
  client = storage.Client()
  bucket= client.get_bucket(event['bucket'])
  blob = bucket.get_blob(event['name'])
  fname=event['name']
  myurl='gs://dump-amazon-brand-analytics-search-terms-12345/'+str(fname)
  print('Uploaded file: {}'.format(event['name']))
  df=pd.read_csv(myurl)
  print('url is {}'.format(myurl))
  print('chunk shape is {}'.format(df.shape))
  date_until=datetime.today().strftime('%Y-%m-%d')
  filename=('{}-Top1MSearchTerms-AmazonUS.csv'.format(date_until))
  cols=get_cols_list()
  if df.columns[4] not in cols:
    f = StringIO()
    df.to_csv(f, index=False)
    f.seek(0)
    client=storage.Client()
    bucket=client.get_bucket('processing-amazon-brand-analytics-search-terms-12345')
    newblob=bucket.blob(filename)
    newblob.upload_from_string(f.read(), content_type='text/csv')
    print('uploaded storage')
    blob.delete()
    print("Blob {} deleted.".format(blob))
  else:
    blob.delete()
    print("Blob {} deleted.".format(blob))
  return f'check the results in the logs'