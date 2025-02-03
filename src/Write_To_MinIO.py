########################################## 
###  This file uploads all parquet files in the Data directory
##########################################

from minio import Minio
import os

def Write_To_MinIO():

    minioClient = Minio( # Client MinIO 
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    bucket = "transactions-bucket"  

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

                except Exception as e:
                    print(f"Unexpected error uploading {file_path}: {e}")
                    return 0
        return 1
    except Exception as e:
        print("\033[1;31m        ########    Problem Occurred While Uploading Data To MinIO\033[0m")
        print(e)
        return 0



if __name__ == "__main__":
    Write_To_MinIO()