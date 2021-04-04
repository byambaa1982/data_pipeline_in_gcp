from google.cloud import storage
import pandas as pd
from datetime import datetime
from io import StringIO
from io import BytesIO
import os

os.environ["GCLOUD_PROJECT"] = "twittersheet-275317"
project_id='twittersheet-275317'
destination_bucket='getting-termites-tweet'

def change_and_save_csv_file(event,destination_bucket):
# it is mandatory initialize the storage client
    client = storage.Client()
    #please change the file's URI
    fname=event['name']
    myurl='gs://getting-termites-tweet/'+str(fname)
    print('Uploaded file: {}'.format(event['name']))
    df=pd.read_csv(myurl)
    print('url is {}'.format(myurl))
    print('chunk shape is {}'.format(df.shape))
    date_until=datetime.today().strftime('%Y-%m-%d')
    filename=('test_v4_{}.csv'.format(date_until))
    try:
        cols=df.columns.to_list()
        if "Daily" in cols[3]:
            a=cols[4].split("[")[1]
            b=a.split(" ")[0]
            df.rename(columns=df.iloc[0])
            df=df.drop(df.index[0])
            df['Report Date']=b
            df=df[['Report Date','Department=[Amazon.com]', 'Search Term=[]',
                    'Enter ASINs or Products=[]', 'Reporting Range=[Daily]',
                    'Viewing=[3/20/21 - 3/20/21]', 'Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7',
                    'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12',
                    'Unnamed: 13', 'Unnamed: 14']]
            df.columns=pd.Series(['Report Date', 'Department', 'Search Term', 'Search Frequency Rank',
                    '#1 Clicked ASIN', '#1 Product Title', '#1 Click Share',
                    '#1 Conversion Share', '#2 Clicked ASIN', '#2 Product Title',
                    '#2 Click Share', '#2 Conversion Share', '#3 Clicked ASIN',
                    '#3 Product Title', '#3 Click Share', '#3 Conversion Share'])
            print(filename)

            f = StringIO()
            df.to_csv(f, index=False)
            f.seek(0)
            client=storage.Client()
            bucket=client.get_bucket('my-image-data-bucket-2021')
            newblob=bucket.blob(filename)
            newblob.upload_from_string(f.read(), content_type='text/csv')
            print('uploaded storage')
        else:
            print("There is no Daily")
    except:
        print('something wrong')
    return f'check the results in the logs'
