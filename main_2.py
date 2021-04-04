from google.cloud import storage
import pandas as pd
import datetime
from io import StringIO
from io import BytesIO
import os

project_id=['YOUR-PROJECT-ID']
destination_bucket=['YOUR-DESTINATION-BUCKET']


def save_to_bq_table():
    bq_client = bigquery.Client()
# Saving data to a intermediate table then export it to GCS<br />query = "
##Query with millions of records results##"<br />
    job_config = bigquery.QueryJobConfig()
# Set the destination table<br />
    table_ref = bq_client.dataset(dataset_id).table('TableID')
    job_config.destination = table_re
    job_config.allow_large_results = True
    # Start the query, passing in the extra configuration.
    query_job = bq_client.query(query,location='US')
     # Location must match that of the source table<br />job_config=job_config) 
     # API request - starts the query<br />query_job.result() 
     # Waits for the query to finish<br />

def export_bq_table():
    client = bigquery.Client()
    destination_uri = 'gs://{}/{}'.format('BucketName','ExportFileName_*.csv')
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(tableId)
    extract_job = client.extract_table(table_ref,destination_uri,location='US') 
# API request # Location must match that of the source table<br />extract_job.result() 
# Waits for job to complete.<br />client.delete_table(table_ref) # API request<br />
from pyexcelerate import Workbook, Range
import pandas as pd
import timeit
import math
import numpy as np

def toPandasExcel(file, frame):
    start = timeit.default_timer()
    frame.to_excel(file, index=False, header=False)
    end = timeit.default_timer()
    delta = round(end-start,2)
    print("to_excel took "+str(delta)+" secs")
    return delta

# Iterate over rows
def toExcelerate1(file, frame):
    start = timeit.default_timer()
    wb = Workbook()
    ws = wb.new_sheet("test")    
    col_num = frame.shape[1]
    row_num = 1
    for row_tup in frame.itertuples(name=None, index=False):
        ws.range((row_num,1), (row_num,col_num)).value = [[*row_tup]]
        row_num += 1        
    wb.save(file)
    end = timeit.default_timer()
    delta = round(end-start,2)
    print("pyexcelerate1 took "+str(delta)+" secs")
    return delta

# Iterate over columns
def toExcelerate2(file, frame):
    start_time = timeit.default_timer()
    wb = Workbook()
    ws = wb.new_sheet("test")    
    row_num = frame.shape[0]    
    col_num = 1
    for col_name, col_series in frame.iteritems():
        ws.range((1,col_num), (row_num,col_num)).value = list(map(lambda a:[a],col_series))
        col_num += 1        
    wb.save(file)
    end_time = timeit.default_timer()
    delta = round(end_time-start_time,2)
    print("pyexcelerate2 took "+str(delta)+" secs")
    return delta
    
# Iterate over columns
def toExcelerate3(file, frame):
    start_time = timeit.default_timer()
    wb = Workbook()
    ws = wb.new_sheet("test")    
    row_num = frame.shape[0]    
    col_num = frame.shape[1]
    ws.range((1,1), (row_num,col_num)).value = frame.values.tolist()
    wb.save(file)
    end_time = timeit.default_timer()
    delta = round(end_time-start_time,2)
    print("pyexcelerate3 took "+str(delta)+" secs")
    return delta
    
def perf(base, eps):
    perc = (base-eps)/base
    print("{0:.2%} over base time".format(perc))
    
print("BUILDING DATAFRAME")
frame = pd.DataFrame()
rows = int(math.pow(10,5))
cols = 5000
dic = dict()
for x in range(cols):
    frame[str(x)] = pd.Series(np.random.randn(rows))

print("to_excel()")
base = toPandasExcel("pandas.xlsx", frame)
print("pyexcelerate1")
e1 = toExcelerate1("pyexcelerate1.xlsx", frame)
perf(base,e1)
print("pyexcelerate2")
e2 = toExcelerate2("pyexcelerate2.xlsx", frame)
perf(base,e2)
print("pyexcelerate3")
e3 = toExcelerate3("pyexcelerate3.xlsx", frame)
perf(base,e3)