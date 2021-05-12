from google.cloud import storage
import pandas as pd
from datetime import datetime
from io import StringIO
from io import BytesIO
import os
import calendar
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

os.environ["GCLOUD_PROJECT"] = "twittersheet-275317"
project_id='twittersheet-275317'
destination_bucket='getting-termites-tweet'


def check_and_trans(event,destination_bucket):
  client = storage.Client()
  #please change the file's URI
  fname=event['name']
  print('Uploaded file: {}'.format(event['name']))
  myurl='gs://staging.twittersheet-275317.appspot.com/'+str(fname)
  bucket=client.get_bucket('getting-termites-tweet')
  files=[]
  for blob in bucket.list_blobs():
    if '.avro' in blob.name:
      print(blob.name)
      filename=blob.name
      files.append(filename)

  print(files[-1])
  my_file=str(files[-1])
  blob=bucket.blob(my_file)
  blob.download_to_filename("temp.avro")
  reader = DataFileReader(open("temp.avro", "rb"), DatumReader())
  records = [r for r in reader]
  # Populate pandas.DataFrame with records
  df = pandas.DataFrame.from_records(records)
  print('url is {}'.format(myurl))
  print('chunk shape is {}'.format(df.shape))
  date_until=datetime.today().strftime('%Y-%m-%d')
  filename=('test_v7_{}.csv'.format(date_until))
  print(filename)
  f = StringIO()
  df2=df[['ip_address', 'date_time','advertiser_id','line_item_id','event_type']]
  df2['temp_date']=df2.date_time.map(lambda x:datetime.fromtimestamp(x))
  df2['Timestamp']=df2.temp_date.map(lambda my_date: '{}, {}'.format(calendar.day_name[my_date.weekday()], my_date.strftime("%b %d, %Y")))
  df2=df2[['ip_address', 'date_time','advertiser_id','line_item_id','event_type']]
  df2.to_csv(f, index=False)
  f.seek(0)
  client=storage.Client()
  bucket=client.get_bucket('getting-termites-tweet')
  newblob=bucket.blob(filename)
  newblob.upload_from_string(f.read(), content_type='text/csv')
  print('uploaded storage')
  return f'check the results in the logs'