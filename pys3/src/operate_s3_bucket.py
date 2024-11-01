import os

import boto3

import pys3


def create_session():
    access = os.getenv(pys3.ACCESS_KEY)
    secret = os.getenv(pys3.SECRET_KEY)
    session = boto3.Session(
        aws_access_key_id=access,
        aws_secret_access_key=secret,
    )
    s3 = session.resource('s3')
    return s3


def main():
    s3 = create_session()
    aws_region = 'eu-north-1'
    bucket_name_a = "al3x3i2023-temp-a"
    bucket_name_b = "al3x3i2023-temp-b"

    # 1
    pys3.create_bucket(bucket_name_a, s3, aws_region, False)
    main_folder = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    upload_dir = os.path.join(main_folder, "upload")
    pys3.upload_file(bucket_name_a, upload_dir, pys3.FILE_1, s3)

    # 2
    pys3.create_bucket(bucket_name_b, s3, aws_region, False)
    pys3.copy_file_from_bucket_to_bucket(bucket_name_a, bucket_name_b, pys3.FILE_1, pys3.FILE_1, s3)

    files = pys3.list_objects_in_bucket(bucket_name_b, s3)
    for file in files:
        print(file)

    pys3.generate_download_link(bucket_name_b, pys3.FILE_1, 30, s3)

    # 3
    pys3.delete_files(bucket_name_a, [pys3.FILE_1], s3)
    pys3.delete_files(bucket_name_b, [pys3.FILE_1], s3)

    pys3.delete_bucket(bucket_name_a, s3)
    pys3.delete_bucket(bucket_name_b, s3)

if __name__ == "__main__":
    main()
