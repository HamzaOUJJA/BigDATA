########################################## 
###  This file uploads all parquet files in the Data directory to MinIO, 
###  then deletes all parquet, crc, metadata files
##########################################

from minio import Minio
import os
import shutil


def Write_To_MinIO():

    minioClient = Minio( # Client MinIO
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    bucket = "transactions"  

    # Create the bucket if it doesn't exist  
    if not minioClient.bucket_exists(bucket):
        minioClient.make_bucket(bucket)      
        print(f"Bucket '{bucket}' created.") 
    
    baseDir = os.path.abspath("../Data")  # Path to the folder containing the parquet files

    try:
        # Only consider files directly in the Data folder
        for file in os.listdir(baseDir):  
            file_path = os.path.join(baseDir, file)
            
            # Check if the file has a .parquet extension
            if os.path.isfile(file_path) and file.endswith(".parquet"):
                object_name = file  # Preserve file name, no folder structure
                
                try:
                    # Upload the file to MinIO
                    minioClient.fput_object(bucket, object_name, file_path)
                    print(f"\033[38;5;214mUploaded To MinIO: {object_name}\033[0m")

                    # Delete the file after successful upload
                    os.remove(file_path)
                    print(f"\033[32mDeleted: {object_name} from local storage\033[0m")

                except Exception as e:
                    print(f"Unexpected error uploading {file_path}: {e}")
                    return 0
                
        # Delete .crc files
        for file in os.listdir(baseDir):
            if file.endswith(".crc"):
                crc_path = os.path.join(baseDir, file)
                os.remove(crc_path)
                print(f"\033[33mDeleted CRC file: {file}\033[0m")

        # Delete metadata directories
        for meta_folder in ["_spark_metadata", "metadata"]:
            meta_path = os.path.join(baseDir, meta_folder)
            if os.path.exists(meta_path) and os.path.isdir(meta_path):
                shutil.rmtree(meta_path)
                print(f"\033[31mDeleted metadata folder: {meta_folder}\033[0m")

        return 1
    except Exception as e:
        print("\033[1;31m        ########    Problem Occurred While Uploading Data To MinIO\033[0m")
        print(e)
        return 0



if __name__ == "__main__":
    Write_To_MinIO()