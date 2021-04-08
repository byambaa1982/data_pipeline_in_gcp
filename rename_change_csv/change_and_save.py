from google.cloud import storage
import pandas as pd
from datetime import datetime
from io import StringIO
from io import BytesIO
import os


os.environ["GCLOUD_PROJECT"] = "juva-brands"
project_id='juva-brands'
destination_bucket='final-amazon-brand-analytics-search-terms-12345'

# -- This function do the followings:
# 1. Read data from processig bucket when a new file comes in
# 2. Check if there is "Daily" in columns value
# 3. Get date after Daily if there is "Daily" and change the format to YYYYMMDD
# 4. Remove the sub header if there is the second header and create a new column using date
# 5. Change schema 
# 6. Change file name
# 7. Save CSV file in the final storage

def change_and_save_csv_file(event,destination_bucket):
# it is mandatory initialize the storage client
  client = storage.Client()
  #please change the file's URI
  fname=event['name']
  myurl='gs://processing-amazon-brand-analytics-search-terms-12345/'+str(fname)
  print('Uploaded file: {}'.format(event['name']))
  # 1 starts here --
  df=pd.read_csv(myurl)
  print('url is {}'.format(myurl))
  print('chunk shape is {}'.format(df.shape))
  try:
    cols=df.columns.to_list()
    #2 starts here --
    if "Daily" in cols[3]:
    #3 starts here --
      a=cols[4].split("[")[1]
      b=a.split(" ")[0]
      my_year=b.split("/")[2]
      my_day=b.split("/")[1]
      my_month=b.split("/")[0]
      if len(my_year)==2:
        my_year=str(20)+my_year
      if len(my_day)==1:
        my_day=str(0)+my_day
      if len(my_month)==1:
        my_month=str(0)+my_month
      print("{}{}{}".format(my_year,my_month,my_day))
      my_date=("{}{}{}".format(my_year,my_month,my_day))
      df.rename(columns=df.iloc[0])
    #4 starts here --
      if df.iloc[0][0]=='Department':
        df=df.drop(df.index[0])
      else:
        pass
      df['Report Date']=my_date
      cols=df.columns.to_list()
      cols = cols[-1:] + cols[:-1]
      df=df[cols]
    # 5 starts here --
      new_cols=['report_date',
                'department',
                'search_terms',
                'search_frequency_rank',
                'first_clicked_asin',
                'first_product_title',
                'first_click_share',
                'first_conversion_share',
                'second_clicked_asin',
                'second_product_title',
                'second_click_share',
                'second_conversion_share',
                'third_clicked_asin',
                'third_product_title',
                'third_click_share',
                'third_conversion_share']
      df.columns=pd.Series(new_cols)
      # 6 start here   
      filename=('{}-Top1MSearchTerms-AmazonUS.csv'.format(my_date))
      print(filename)
      # 7 start here 
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