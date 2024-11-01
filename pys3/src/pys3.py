#!/usr/bin/env python3

from botocore.exceptions import ClientError
import os
import boto3


# -*-coding:utf-8-*-
ACCESS_KEY = 'AWS_ACCESS_KEY'
SECRET_KEY = 'AWS_SECRET_ACCESS_KEY'
PRIVATE_BUCKET_NAME = 'al3x3i2023'
TRANSIENT_BUCKET_NAME = 'al3x3i2023-v2'
SECURE_BUCKET_NAME = 'al3x3i2023-secure'
PUBLIC_BUCKET_NAME = 'al3x3i2023-public'
FILE_1 = 'alx1.txt'
FILE_2 = 'alx2.txt'
FILE_3 = 'alx3.txt'

"""This is the script to work with AWS S3"""


def create_bucket(name, s3, region, secure=False):
    try:
        s3.create_bucket(Bucket=name, CreateBucketConfiguration={
            'LocationConstraint': region
        })

        if secure:
            prevent_public_access(name, s3)
        else:
            grant_public_access(name, s3)

    except ClientError as ce:
        print('error', ce)


def prevent_public_access(bucket_name, s3):
    try:
        s3.meta.client.put_public_access_block(Bucket=bucket_name,
                                               PublicAccessBlockConfiguration={
                                                   'BlockPublicAcls': True,
                                                   'IgnorePublicAcls': True,
                                                   'BlockPublicPolicy': True,
                                                   'RestrictPublicBuckets': True
                                               })
    except ClientError as ce:
        print('error', ce)


def grant_public_access(bucket_name, s3):
    try:
        s3.meta.client.put_public_access_block(Bucket=bucket_name,
                                               PublicAccessBlockConfiguration={
                                                   'BlockPublicAcls': False,
                                                   'IgnorePublicAcls': False,
                                                   'BlockPublicPolicy': False,
                                                   'RestrictPublicBuckets': False
                                               })
    except ClientError as ce:
        print('error', ce)


def delete_bucket(bucket_name, s3):
    try:
        s3.Bucket(bucket_name).delete()

    except ClientError as ce:
        print('error', ce)


def generate_download_link(bucket_name, file_key, expiration_time_sec, s3):
    try:
        response = s3.meta.client.generate_presigned_url('get_object', Params={
            'Bucket': bucket_name,
            'Key': file_key
        }, ExpiresIn=expiration_time_sec)
        print(f'File download link: {response}')
    except ClientError as ce:
        print('error', ce)


def upload_file(bucket, directory, file, s3, s3path=None):
    file_path = directory + '/' + file
    remote_path = s3path
    if remote_path is None:
        remote_path = file

    try:
        s3.Bucket(bucket).upload_file(file_path, remote_path)
    except ClientError as ce:
        print('error', ce)


def download_file(bucket, directory, local_name, key_name, s3):
    file_path = directory + '/' + local_name
    try:
        s3.Bucket(bucket).download_file(key_name, file_path)

    except ClientError as ce:
        print('error', ce)


def delete_files(bucket, keys, s3):
    objects = []
    for key in keys:
        objects.append({'Key': key})

    try:
        s3.Bucket(bucket).delete_objects(Delete={'Objects': objects})
    except ClientError as ce:
        print('error', ce)


def copy_file_from_bucket_to_bucket(source_bucket, dest_bucket, source_key, dest_key, s3):
    try:
        source = {
            'Bucket': source_bucket,
            'Key': source_key
        }
        s3.Bucket(dest_bucket).copy(source, dest_key)

    except ClientError as ce:
        print('error', ce)


def list_objects_in_bucket(bucket, s3):
    try:
        response = s3.meta.client.list_objects(Bucket=bucket)
        objects = []
        for content in response['Contents']:
            objects.append(content['Key'])
            print(bucket, 'contains', len(objects), 'files')
        return objects
    except ClientError as ce:
        print('error', ce)


def main():
    """entry point"""
    # access = os.getenv(ACCESS_KEY)
    # secret = os.getenv(SECRET_KEY)
    # aws_region = 'eu-north-1'
    #
    # main_folder = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    # upload_dir = os.path.join(main_folder, "upload")
    # download_dir = os.path.join(main_folder, "download")
    #
    # session = boto3.Session(
    #     aws_access_key_id=access,
    #     aws_secret_access_key=secret,
    # )
    #
    # # Use the session to create a resource
    # s3 = session.resource('s3')
    #
    # # # A.Create Bucket
    # create_bucket(PRIVATE_BUCKET_NAME, s3, aws_region, True)

    # # B. Create secure Bucket
    # create_bucket(SECURE_BUCKET_NAME, s3, aws_region, True)

    # # C. Create public Bucket
    # create_bucket(PUBLIC_BUCKET_NAME, s3, aws_region, False)
    # upload_file(PUBLIC_BUCKET_NAME, upload_dir, FILE_1, s3)
    # generate_download_link(PUBLIC_BUCKET_NAME, FILE_1, 30, s3)

    # # Upload files
    # upload_file(PRIVATE_BUCKET_NAME, upload_dir, FILE_1, s3)
    # upload_file(PRIVATE_BUCKET_NAME, upload_dir, FILE_2, s3)
    # upload_file(PRIVATE_BUCKET_NAME, upload_dir, FILE_3, s3)

    # # Download file from S3 Bucket
    # download_file(PRIVATE_BUCKET_NAME, download_dir, FILE_3, FILE_3, s3)
    #
    # # Delete files in S3 Bucket
    # delete_file3(PRIVATE_BUCKET_NAME, [FILE_1, FILE_2, FILE_3], s3)

    # # Copy files from one Bucket to another
    # create_bucket(TRANSIENT_BUCKET_NAME, s3, aws_region, True)
    # copy_file_from_bucket_to_bucket(PRIVATE_BUCKET_NAME, TRANSIENT_BUCKET_NAME, FILE_2, FILE_2, s3)
    # list_objects_in_bucket(TRANSIENT_BUCKET_NAME, s3)

    # Delete Bucket
    # create_bucket(TRANSIENT_BUCKET_NAME, s3, aws_region, True)
    # delete_bucket(TRANSIENT_BUCKET_NAME, s3)


if __name__ == "__main__":
    main()
