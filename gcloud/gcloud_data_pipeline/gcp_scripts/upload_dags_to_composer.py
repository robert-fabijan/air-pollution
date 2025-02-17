from __future__ import annotations

import argparse
import glob
import json
import os
from shutil import copytree, ignore_patterns
import tempfile

# Imports the Google Cloud client library
from google.cloud import storage
from google.oauth2 import service_account


def _get_storage_client():
    """
    Method that loads credential provided through secrets on Google Cloud. 

    `cloudbuild.yaml` retrieves json file from secrets and saves it as environmental variable 
    for image to use it 

    Returns:
        google storage client
    """
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if credentials_json:
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        return storage.Client(credentials=credentials)
    else:
        return storage.Client()



def _create_files_list(dags_directory: str) -> tuple[str, list[str]]:
    """
    Create a temporary list for dags files to being uploaded to storage bucket
    """
    temp_dir = tempfile.mkdtemp()

    # ignore non-DAG Python files
    files_to_ignore = ignore_patterns("*_test.py")

    # Copy everything but the ignored files to a temp directory
    copytree(dags_directory, f"{temp_dir}/", ignore=files_to_ignore, dirs_exist_ok=True)

    # The only Python files left in our temp directory are DAG files
    # so we can exclude all non Python files
    dags = glob.glob(f"{temp_dir}/*.py")
    return (temp_dir, dags)



def upload_files_to_bucket(
    dags_directory: str, bucket_name: str, name_replacement: str = "dags"
) -> None:
    """
    Given a directory, this function moves all DAG files from that directory
    to a temporary directory, then uploads all contents of the temporary directory
    to a given cloud storage bucket..
    Args:
        dags_directory (str): a fully qualified path to a directory that contains a "dags/" subdirectory
        bucket_name (str): the GCS bucket of the Cloud Composer environment to upload DAGs to
        name_replacement (str, optional): the name of the "dags/" subdirectory that will be used when constructing the temporary directory path name Defaults to "dags/".
    """
    temp_dir, dags = _create_files_list(dags_directory)

    if len(dags) > 0:
        # Note - the GCS client library does not currently support batch requests on uploads
        # if you have a large number of files, consider using
        # the Python subprocess module to run gsutil -m cp -r on your dags
        # See https://cloud.google.com/storage/docs/gsutil/commands/cp for more info
        storage_client = _get_storage_client()
        bucket = storage_client.bucket(bucket_name)

        for dag_path in dags:
            # Extract the filename from the path
            dag_filename = os.path.basename(dag_path)
            # The path to use for blob should be the relative path within the bucket
            blob_path = os.path.join(name_replacement+'/', dag_filename)
            # The actual file to upload is in the temp directory
            local_dag_path = os.path.join(temp_dir, dag_filename)

            try:
                # Upload to your bucket
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(local_dag_path)
                print(f"File {local_dag_path} uploaded to {bucket_name}/{blob_path}.")
            except FileNotFoundError as e:
                print(f"Failed to upload {local_dag_path}: {e}")
                raise

    else:
        print("No DAGs to upload.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dags_directory",
        help="Relative path to the source directory containing your DAGs",
    )
    parser.add_argument(
        "--dags_bucket",
        help="Name of the DAGs bucket of your Composer environment without the gs:// prefix",
    )
    parser.add_argument(
        "--bucket_folder_name",
        help="Name of folder inside specified bucket from --dags_bucket",
    )

    args = parser.parse_args()

    upload_files_to_bucket(
        dags_directory = args.dags_directory, 
        bucket_name = args.dags_bucket, 
        name_replacement = args.bucket_folder_name
    )
