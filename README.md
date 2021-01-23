---
title: "How to do ETL using Google Cloud Function?"
date: 2021-01-22 11:33:00 +0800
categories: [Google Cloud Function, Python]
tags: [pandas, lambda, cloud engineering]
---
[![Python Version][python-image]](https://img.shields.io/github/languages/code-size/byambaa1982/data_pipeline_in_gcp)

# How to do ETL using Google Cloud Function?

All code is in [my github](https://github.com/byambaa1982/data_pipeline_in_gcp/blob/main/main.py)

## Goal
Build reliable serverless and cost effective data pipelines in GCP using python.
Thankfully, Google Cloud (GCP) offers some awesome serverless tools where you can run a workflow like this for next to no cost


## The Workflow

![Alt text](https://storage.googleapis.com/my-bigquery-lab-286400_cloudbuild/images/data%20mining%202.png "Data Pipeline")

1. Create three buckets in Cloud Storage

Setting up a Cloud Storage bucket is pretty straightforward, so straightforward that I’ll just give you a link to [the official GCP documentation](https://cloud.google.com/storage/docs/creating-buckets) that gives an example.
The first storage is a temporary storage that for uploading a compressed file and triggering the first cloud function.
The second one is a staging storage that for triggering the second cloud function. 
The third one is the final storage where we can download result excel files files. 

2. Create 2 Cloud Function. 

The first cloud function do extact a compressed file, sent it to the second storage and delete one in the first storage. That way, we can avoid incurring unnessery storage fee. 
The second cloud function do ETL. It will chunk large csv file into small Excel files and store them in the final storage. 

If you have anything to ask, please contact me clicking following link?


You can hire me [here](https://www.fiverr.com/coderjs)

Thank you