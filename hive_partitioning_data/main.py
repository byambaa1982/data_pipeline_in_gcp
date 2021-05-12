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
  df2=df[['ip_address', 'date_time']]
  df2['temp_date']=df2.date_time.map(lambda x:datetime.fromtimestamp(x))
  df2['Timestamp']=df2.temp_date.map(lambda my_date: '{}, {}'.format(calendar.day_name[my_date.weekday()], my_date.strftime("%b %d, %Y")))
  df2=df2[['ip_address', 'Timestamp']]
  f = StringIO()
  df2.to_csv(f, index=False)
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

  return f'check the results in the logs'