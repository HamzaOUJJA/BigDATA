##########################################
###  This file uploads in real time all parquet files in the Data directory to MinIO, 
###  It observes using "watchdog" the ../Data folder every 10 sec for new files to upload to MinIO
##########################################

from minio import Minio
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# MinIO Client Setup
minioClient = Minio(
    "localhost:9000",
    secure=False,
    access_key="minio",
    secret_key="minio123"
)

bucket = "transactions"

# Ensure the bucket exists
if not minioClient.bucket_exists(bucket):
    minioClient.make_bucket(bucket)
    print(f"\033[38;5;214mBucket '{bucket}' created.\033[0m")

baseDir = os.path.abspath("../Data")


class FileHandler(FileSystemEventHandler):
    """ Watches for new files and uploads them to MinIO """

    def on_created(self, event):
        if event.is_directory:
            return  # Ignore directories

        file_path = event.src_path
        file_name = os.path.basename(file_path)

        # Process only .parquet files
        if file_name.endswith(".parquet"):
            self.upload_to_minio(file_name, file_path)

    def upload_to_minio(self, file_name, file_path):
        """ Uploads the file to MinIO """
        try:
            minioClient.fput_object(bucket, file_name, file_path)
            print(f"\033[38;5;214mUploaded To MinIO: {file_name}\033[0m")

        except Exception as e:
            print(f"\033[1;31mError uploading {file_name}: {e}\033[0m")


def upload_existing_files():
    """ Upload all existing Parquet files before starting the observer """
    print("\033[1;36m\033[1mChecking for existing Parquet files...\033[0m")
    for file_name in os.listdir(baseDir):
        file_path = os.path.join(baseDir, file_name)

        if file_name.endswith(".parquet") and os.path.isfile(file_path):
            try:
                minioClient.fput_object(bucket, file_name, file_path)
                print(f"\033[38;5;214mUploaded To MinIO: {file_name}\033[0m")
            except Exception as e:
                print(f"\033[1;31mError uploading {file_name}: {e}\033[0m")


if __name__ == "__main__":
    # Upload existing files before starting the observer
    upload_existing_files()

    # Start the watchdog observer
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, baseDir, recursive=False)  # Watch the Data folder

    print("\033[1;36m\033[1m    👀 Watching for new Parquet files to upload\033[0m")
    observer.start()

    try:
        while True:
            time.sleep(10)  
    except KeyboardInterrupt:
        observer.stop()
        print("\033[1;31mStopped watching.\033[0m")
    
    observer.join()
