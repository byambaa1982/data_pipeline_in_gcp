import logging
import os

from flask import Flask, request
from google.cloud import storage
import pandas as pd
from io import BytesIO
import time

app = Flask(__name__)

# Configure this environment variable via app.yaml
CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']


@app.route('/')
def index():
    return """
<form method="POST" action="/upload" enctype="multipart/form-data">
    <input type="file" name="file">
    <input type="submit">
</form>
"""


@app.route('/upload', methods=['POST'])
def upload():
    """Process the uploaded file and upload it to Google Cloud Storage."""
    uploaded_file = request.files.get('file')
    if not uploaded_file:
        return 'No file uploaded.', 400
    # Create a Cloud Storage client.
    gcs = storage.Client()
    # Get the bucket that the file will be uploaded to.
    bucket = gcs.get_bucket(CLOUD_STORAGE_BUCKET)
    # df=pd.read_csv(uploaded_file, encoding='utf-8')
    for i,chunk in enumerate(pd.read_csv(uploaded_file, encoding='utf-8',chunksize=2)):
        try:
            newfilename=('chunk{}.xlsx'.format(i))
            blob = bucket.blob(newfilename)
            f= BytesIO()  
            chunk.to_excel(f)
            f.seek(0)
            blob.upload_from_string(f.read(),content_type='xlsx')
            # return blob.public_url
        except:
            return('something wrong: {}'.format(i))
    return 'check your cloud storage bucket'

@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_flex_storage_app]
