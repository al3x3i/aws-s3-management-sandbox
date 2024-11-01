# This is my sandbox project to work with AWS S3. 
This is an example code how to communicate with the AWS S3 

# How to


## AWS configurations
.aws/config
````shell
[profile personal]
region = eu-north-1
output = json
````

.aws/credentials
```sghell
[personal]
aws_access_key_id =
aws_secret_access_key =
```

## Install dependencies
```shell
source venv/bin/activate
pip install -r requirements.txt
```

# Application Structure

## Shell script
* Create Bucket
* Upload file
* Sync files with another Bucket
* Create Temporary URL link to the file
* List all files

## Python
The application is capable to do:
* Create Bucket
* Upload files
* Download files
* Delete files
* Copy files from one S3 Bucket to another S3 Bucket
* List objects in S3 Bucket
* Bucket security (Create secure Bucket)
* Create Temporary URL link to the file

## Java
The Java module uses the AWS SDK for Java to interact with S3. 
It includes the following functionalities:
* Create Bucket
* Upload Files
* Download Files
* Delete Files
* Copy Files Between Buckets
* List Objects
* Bucket Security
* Create Temporary URL

