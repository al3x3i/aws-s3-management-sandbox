#!/bin/bash

# Create bucket

aws --profile personal s3 mb s3://al3x3i0207

# Create dummy files
echo "Hello S3, first time" >> f1.txt
echo "Hello S3, second time" >> f2.txt


# Upload just created files to S3
aws --profile personal s3 cp f1.txt s3://al3x3i0207
aws --profile personal s3 cp f2.txt s3://al3x3i0207


# Sync all files in folder with the S3
#aws s3 sync ./ s3://al3x3i0207

# Create temporary URL link to a S3 file for 30 seconds:
# aws s3 presign s3://al3x3i0207/f1.txt --expires-in 30

# List S3 files
aws --profile personal s3 ls s3://al3x3i0207

