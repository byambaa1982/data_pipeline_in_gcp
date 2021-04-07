from google.cloud import storage
import pandas as pd
from datetime import datetime
from io import StringIO
from io import BytesIO
import os
import pandas_gbq

os.environ["GCLOUD_PROJECT"] = "juva-brands"
destination_bucket='final-amazon-brand-analytics-search-terms-12345'

def load_df_to_bq(event,destination_bucket):
  client = storage.Client()
  #please change the file's URI
  fname=event['name']
  myurl='gs://final-amazon-brand-analytics-search-terms-12345/'+str(fname)
  print('Uploaded file: {}'.format(event['name']))
  df=pd.read_csv(myurl)
  print('url is {}'.format(myurl))
  print('df shape is {}'.format(df.shape))
  cols=df.columns.to_list()
  bq_cols=[col.replace(" ","_") for col in cols]
  df_bq=df.copy()
  df_bq.columns=pd.Series(bq_cols)
  project_id='juva-brands'
  table_id='bucket_search_terms.amazon_search_term_competitive_metrics'
  print('cols are {}'.format(cols))
  pandas_gbq.to_gbq(df_bq, table_id, project_id=project_id ,if_exists='append')
  print('injected df into bq')
  return f'check the results in the logs'


