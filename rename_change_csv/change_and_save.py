from google.cloud import storage
import pandas as pd
from datetime import datetime
from io import StringIO
from io import BytesIO
import os


os.environ["GCLOUD_PROJECT"] = "juva-brands"
project_id='juva-brands'
destination_bucket='final-amazon-brand-analytics-search-terms-12345'

def change_and_save_csv_file(event,destination_bucket):
# it is mandatory initialize the storage client
  client = storage.Client()
  #please change the file's URI
  fname=event['name']
  myurl='gs://processing-amazon-brand-analytics-search-terms-12345/'+str(fname)
  print('Uploaded file: {}'.format(event['name']))
  df=pd.read_csv(myurl)
  print('url is {}'.format(myurl))
  print('chunk shape is {}'.format(df.shape))
  try:
    cols=df.columns.to_list()
    if "Daily" in cols[3]:
      a=cols[4].split("[")[1]
      b=a.split(" ")[0]
      my_year=b.split("/")[2]
      my_day=b.split("/")[1]
      my_month=b.split("/")[0]
      if len(my_day)==1:
        my_day=str(0)+my_day
      if len(my_month)==1:
        my_month=str(0)+my_month
      print("{}{}{}".format(my_year,my_month,my_day))
      my_date=("{}{}{}".format(my_year,my_month,my_day))
      df.rename(columns=df.iloc[0])
      df=df.drop(df.index[0])
      df['Report Date']=my_date
      cols=df.columns.to_list()
      cols = cols[-1:] + cols[:-1]
      df=df[cols]
      new_cols=['Report_Date', 'Department', 'Search_Term', 'Search_Frequency_Rank',
       'Clicked_ASIN_1', 'Product_Title_1', 'Click_Share_1',
       'Conversion_Share_1', 'Clicked_ASIN_2', 'Product_Title_2',
       'Click Share_2', 'Conversion Share_2', 'Clicked ASIN_3',
       'Product Title_3', 'Click Share_3', 'Conversion_Share_3']
      df.columns=pd.Series(new_cols)
      # my_date=b.replace("/", "-")     
      filename=('{}-Top1MSearchTerms-AmazonUS.csv'.format(my_date))
      print(filename)
      f = StringIO()
      df.to_csv(f, index=False)
      f.seek(0)
      client=storage.Client()
      bucket=client.get_bucket('final-amazon-brand-analytics-search-terms-12345')
      newblob=bucket.blob(filename)
      newblob.upload_from_string(f.read(), content_type='text/csv')
      print('uploaded storage')
    else:
      print("There is no Daily")
  except:
    print('something wrong')
  return f'check the results in the logs'