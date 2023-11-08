FROM python:3.8

# Allow statements and log messages to immediately appear in the Cloud Run logs
ENV PYTHONUNBUFFERED True

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

#copy dag code to container image
ENV WORK_DIRECTORY ./gcloud/gcloud_data_pipeline/
WORKDIR $WORK_DIRECTORY
COPY . ./

CMD python $WORK_DIRECTORY/upload_dags_to_composer.py --dags_directory=$WORK_DIRECTORY/dags/ --dags_bucket=gs://air-pollution-bucket/dags