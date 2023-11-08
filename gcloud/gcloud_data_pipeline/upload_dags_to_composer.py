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
    from pprint import pprint
    pprint(os.getenv().items())
    print()
    print()
    pprint(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'))
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if credentials_json:
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        return storage.Client(credentials=credentials)
    else:
        return storage.Client()



def _create_dags_list(dags_directory: str) -> tuple[str, list[str]]:
    """
    Create a temporary list for dags files to being uploaded to storage bucket
    """
    temp_dir = tempfile.mkdtemp()

    # ignore non-DAG Python files
    files_to_ignore = ignore_patterns("__init__.py", "*_test.py")

    # Copy everything but the ignored files to a temp directory
    copytree(dags_directory, f"{temp_dir}/", ignore=files_to_ignore, dirs_exist_ok=True)

    # The only Python files left in our temp directory are DAG files
    # so we can exclude all non Python files
    dags = glob.glob(f"{temp_dir}/*.py")
    return (temp_dir, dags)



def upload_dags_to_composer(
    dags_directory: str, bucket_name: str, name_replacement: str = "dags/"
) -> None:
    """
    Given a directory, this function moves all DAG files from that directory
    to a temporary directory, then uploads all contents of the temporary directory
    to a given cloud storage bucket
    Args:
        dags_directory (str): a fully qualified path to a directory that contains a "dags/" subdirectory
        bucket_name (str): the GCS bucket of the Cloud Composer environment to upload DAGs to
        name_replacement (str, optional): the name of the "dags/" subdirectory that will be used when constructing the temporary directory path name Defaults to "dags/".
    """
    temp_dir, dags = _create_dags_list(dags_directory)

    if len(dags) > 0:
        # Note - the GCS client library does not currently support batch requests on uploads
        # if you have a large number of files, consider using
        # the Python subprocess module to run gsutil -m cp -r on your dags
        # See https://cloud.google.com/storage/docs/gsutil/commands/cp for more info
        storage_client = _get_storage_client()
        bucket = storage_client.bucket(bucket_name)

        for dag in dags:
            # Remove path to temp dir
            dag = dag.replace(f"{temp_dir}/", name_replacement)

            try:
                # Upload to your bucket
                blob = bucket.blob(dag)
                blob.upload_from_filename(dag)
                print(f"File {dag} uploaded to {bucket_name}/{dag}.")
            except FileNotFoundError:
                current_directory = os.listdir()
                print(
                    f"{name_replacement} directory not found in {current_directory}, you may need to override the default value of name_replacement to point to a relative directory"
                )
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

    args = parser.parse_args()

    upload_dags_to_composer(args.dags_directory, args.dags_bucket)
