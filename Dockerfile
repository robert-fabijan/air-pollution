FROM python:3.8

# Allow statements and log messages to immediately appear in the Cloud Run logs
ENV PYTHONUNBUFFERED True

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

#copy dag code to container image
ENV DAGS ./gcloud/gcloud_data_pipeline/
WORKDIR $DAGS
COPY . ./

CMD python ./gcloud/gcloud_data_pipeline/upload_dags_to_composer.py --dags_directory=./gcloud/gcloud_data_pipeline/dags/ --dags_bucket=gs://air-pollution-bucket/dags