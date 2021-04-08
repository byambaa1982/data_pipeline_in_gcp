from google.cloud import storage
import pandas as pd
from datetime import datetime
from io import StringIO
from io import BytesIO
import os

os.environ["GCLOUD_PROJECT"] = "juva-brands"
project_id='juva-brands'
destination_bucket='dump-amazon-brand-analytics-search-terms-12345'
# This function do the followings:
#1. Iterate all files in the processig bucket one by one when a new file uploaded into the dump bucket
#2. Get the first rows and make a list
def get_cols_list():
  os.environ["GCLOUD_PROJECT"] = "juva-brands"
  client=storage.Client()
  bucket = client.bucket('processing-amazon-brand-analytics-search-terms-12345')
  cols=[]
  #1 starts here --
  for blob in bucket.list_blobs():
    print(blob.name)
    filename=blob.name
    try: 
  #2 starts here --  
      myurl='gs://processing-amazon-brand-analytics-search-terms-12345/'+str(filename)
      df=pd.read_csv(myurl, nrows=1)
      cols.append(df.columns[4])
      print(df.columns[4])
    except:
      continue
  return cols
# This function do the followings:
# 1. Read the file when a new file uploaded into the dump bucket
# 2. Change file name
# 3. Check if there is duplication
# 4. Save file in the processing bucket
# 5. Delete the file in dump bucket
def check_and_trans_v2(event,destination_bucket):
  client = storage.Client()
  #1 Starts here --
  bucket= client.get_bucket(event['bucket'])
  blob = bucket.get_blob(event['name'])
  fname=event['name']
  myurl='gs://dump-amazon-brand-analytics-search-terms-12345/'+str(fname)
  print('Uploaded file: {}'.format(event['name']))
  df=pd.read_csv(myurl)
  print('url is {}'.format(myurl))
  print('chunk shape is {}'.format(df.shape))
  date_until=datetime.today().strftime('%Y-%m-%d')
  #2 starts here --
  filename=('{}-Top1MSearchTerms-AmazonUS.csv'.format(date_until))
  cols=get_cols_list()
  #3 starts here --
  if df.columns[4] not in cols:
    f = StringIO()
    df.to_csv(f, index=False)
    f.seek(0)
    client=storage.Client()
    bucket=client.get_bucket('processing-amazon-brand-analytics-search-terms-12345')
    newblob=bucket.blob(filename)
  # 5 starts here --
    newblob.upload_from_string(f.read(), content_type='text/csv')
    print('uploaded storage')
  # 6 starts here --
    blob.delete()
    print("Blob {} deleted.".format(blob))
  else:
    blob.delete()
    print("Blob {} deleted.".format(blob))
  return f'check the results in the logs'