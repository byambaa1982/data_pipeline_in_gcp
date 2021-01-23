
# How to do ETL using Google Cloud Function?

![repo size](https://img.shields.io/github/repo-size/byambaa1982/data_pipeline_in_gcp)
![size](https://img.shields.io/github/languages/code-size/byambaa1982/data_pipeline_in_gcp)
![language count](https://img.shields.io/github/languages/count/byambaa1982/data_pipeline_in_gcp)
![social](https://img.shields.io/github/followers/byambaa1982?style=social)
![stats](https://img.shields.io/github/stars/byambaa1982/data_pipeline_in_gcp?style=social)


All code is in [my github](https://github.com/byambaa1982/data_pipeline_in_gcp/blob/main/main.py)

## Goal: ETL or Extract, Transfer, and Load
Build reliable serverless and cost effective data pipelines in GCP using python.
Thankfully, Google Cloud (GCP) offers some awesome serverless tools where you can run a workflow like this for next to no cost.

In this repo, we will look to do the following:

- Set up Cloud Function and Cloud Storage
- Extract data
- Transform data
- Load data
- Automate our pipeline


## The Workflow

![Alt text](https://storage.googleapis.com/my-bigquery-lab-286400_cloudbuild/images/data%20mining%202.png "Data Pipeline")

1. Create three buckets in Cloud Storage

- Setting up a Cloud Storage bucket is pretty straightforward, so straightforward that I’ll just give you a link to [the official GCP documentation](https://cloud.google.com/storage/docs/creating-buckets) that gives an example.
- The first storage is a temporary storage that for uploading a compressed file and triggering the first cloud function.
- The second one is a staging storage that for triggering the second cloud function. 
The third one is the final storage where we can download result excel files files. 

2. Create 2 Cloud Function. 

- The first cloud function do extact a compressed file, sent it to the second storage and delete one in the first storage. That way, we can avoid incurring unnessery storage fee. 
- The second cloud function do ETL. It will chunk large csv file into small Excel files and store them in the final storage. 

If you have anything to ask, please contact me clicking following link?


You can hire me [here](https://www.fiverr.com/coderjs)

Thank you