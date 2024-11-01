package com.management.s3;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.PublicAccessBlockConfiguration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutPublicAccessBlockRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

public class Application {

    public enum ApplicationFeature {
        COPY_DOWNLOAD_FILES,
        COPY_S3_TO_S3_AND_LIST,
        BLOCK_PUBLIC_ACCESS,
        CREATE_PRE_SIGN_URL,
        DELETE_S3_BUCKET,
        CHALLENGE // collection of S3 bucket manipulation actions
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String AWS_ACCESS_KEY = "AWS_ACCESS_KEY_ID";
    private static final String AWS_SECRET_KEY = "AWS_SECRET_ACCESS_KEY";
    private static final String AWS_PROFILE = "personal";
    // My .aws/credentials has "personal" profile with access key and secret key
    private static final String PRIMARY_BUCKET_NAME = "al-lil-bucket";
    private static final String TRANSIENT_BUCKET_NAME = "al-lil-bucket-transient";
    private static final String F1 = "al-lil1.txt";
    private static final String F2 = "al-lil2.txt";
    private static final String F3 = "al-lil3.txt";
    private static final String PROJECT_PATH = System.getProperty("user.dir");
    private static final String GLOBAL_DIR = "bucketFiles";
    private static final String UPLOAD_DIR = "upload";
    private static final String DOWNLOAD_DIR = "download";

    private final S3Client s3Client;

    private S3Presigner presigner;

    public Application(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public void setS3Presigner(@NotNull S3Presigner presigner) {
        this.presigner = presigner;
    }

    public void createBucket(String name) {
        try {
            CreateBucketRequest request = CreateBucketRequest
                .builder()
                .bucket(name)
                .build();

            s3Client.createBucket(request);

            LOGGER.info("Bucket create successfully. {}", name);
        } catch (Exception ex) {
            LOGGER.error("Error: {}", ex.getMessage());
        }
    }

    public boolean bucketExists(String bucketName) {
        try {
            HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();
            s3Client.headBucket(request);
            LOGGER.info("Bucket is already exists. {} \n", bucketName);
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            LOGGER.warn("Error checking bucket exist: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            LOGGER.error("Unexpected Error: {}", e.getMessage());
            return false;
        }
    }

    public void uploadFile(String bucket, String fileName, String localDirector, String key) {
        try {
            PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(key).build();
            s3Client.putObject(request, getFilePath(localDirector, fileName));
            LOGGER.info("File uploaded successfully. {}", fileName);
        } catch (Exception ex) {
            LOGGER.error("Error: {}", ex.getMessage());
        }
    }

    public void downloadFile(String bucket, String fileName, String localDirector, String key) {
        try {

            if (Files.exists(getFilePath(localDirector, fileName))) {
                LOGGER.info("File was already downloaded. Please delete the file and repeat the operation");
                return;
            }

            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
            s3Client.getObject(request, getFilePath(localDirector, fileName));
            LOGGER.info("File downloaded successfully. {}", fileName);
        } catch (Exception ex) {
            LOGGER.error("Error: {}", ex.getMessage());
        }
    }

    public void deleteFile(String bucket, String key) {
        try {
            DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
            s3Client.deleteObject(request);
            LOGGER.info("File deleted successfully. {}", key);
        } catch (Exception e) {
            LOGGER.error("Error: {}", e.getMessage());
        }
    }

    public List<String> listFiles(String bucketName) {
        var keys = new ArrayList<String>();
        try {
            ListObjectsRequest request = ListObjectsRequest.builder().bucket(bucketName).build();
            var response = s3Client.listObjects(request);
            response.contents().forEach(content -> keys.add(content.key()));

            LOGGER.info("Number of files in s3 bucket: {}", keys.size());
            keys.forEach(key -> LOGGER.info("Key: {}", key));
        } catch (Exception e) {
            LOGGER.error("Error: {}", e.getMessage());
        }
        return keys;
    }

    public void copyFilesFromSourceS3toDestS3(
        String sourceBucket,
        String destBucket,
        String sourceKey,
        String destKey
    ) {
        try {
            CopyObjectRequest.builder()
                .copySource(sourceBucket)
                .destinationBucket(destBucket)
                .destinationKey(destKey)
                .build();

            CopyObjectRequest request = CopyObjectRequest.builder()
                .sourceBucket(sourceBucket)
                .sourceKey(sourceKey)
                .destinationBucket(destBucket)
                .key(destKey)
                .build();

            s3Client.copyObject(request);

        } catch (Exception e) {
            LOGGER.error("Error: {}", e.getMessage());
        }
    }

    private static Path getFilePath(final String localDirector, final String fileName) {
        return Paths.get(PROJECT_PATH, List.of(GLOBAL_DIR, localDirector, fileName).toArray(new String[0]));
    }

    private static S3Client createS3Client() {
        //        String accessKey = System.getenv(AWS_ACCESS_KEY);
        //        String privateKey = System.getenv(AWS_SECRET_KEY);

        //        AwsSessionCredentials credentials = AwsSessionCredentials.create(accessKey, privateKey, "");
        //        S3Client s3 = S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(credentials))
        //        .build();

        AwsCredentialsProvider credentials = ProfileCredentialsProvider.create(AWS_PROFILE);
        return S3Client.builder().credentialsProvider(credentials).region(Region.EU_CENTRAL_1).build();
    }

    private static S3Presigner createS3Presigner() {
        AwsCredentialsProvider credentials = ProfileCredentialsProvider.create(AWS_PROFILE);
        return S3Presigner.builder().credentialsProvider(credentials).region(Region.EU_CENTRAL_1).build();
    }

    private void blockAllPublicAccess(final String bucketName) {
        try {
            PutPublicAccessBlockRequest request =
                PutPublicAccessBlockRequest.builder()
                    .bucket(bucketName)
                    .publicAccessBlockConfiguration(PublicAccessBlockConfiguration.builder()
                        .blockPublicAcls(true)
                        .blockPublicPolicy(true)
                        .restrictPublicBuckets(true)
                        .ignorePublicAcls(true)
                        .build())
                    .build();
            s3Client.putPublicAccessBlock(request);

            LOGGER.info("Block all public access to the S3 bucket: {}", bucketName);
        } catch (Exception e) {
            LOGGER.error("Error: {}", e.getMessage());
        }
    }

    private String createPreSignUrl(final String bucketName, final String fileKey) {
        String presignUrl = null;
        try {
            GetObjectPresignRequest request = GetObjectPresignRequest.builder()
                .getObjectRequest(GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileKey)
                    .build()
                )
                .signatureDuration(Duration.ofSeconds(30))
                .build();

            PresignedGetObjectRequest pRequest = presigner.presignGetObject(request);
            presignUrl = pRequest.url().toString();
        } catch (Exception e) {
            LOGGER.error("Error: {}", e.getMessage());
        }
        return presignUrl;
    }

    private void deleteS3Bucket(final String bucketName) {
        try {
            DeleteBucketRequest request = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3Client.deleteBucket(request);

            LOGGER.info("Bucket deleted successfully. {}", bucketName);
        } catch (Exception e) {
            LOGGER.error("Error: {}", e.getMessage());
        }
    }

    public static void main(String[] args) {
        LOGGER.info(System.lineSeparator() + "=".repeat(20));
        S3Client s3 = createS3Client();
        Application app = new Application(s3);

        if (!app.bucketExists(TRANSIENT_BUCKET_NAME)) {
            app.createBucket(TRANSIENT_BUCKET_NAME);
        } else {
            LOGGER.info("Skip bucket creating. {}", TRANSIENT_BUCKET_NAME);
        }

        ApplicationFeature action = ApplicationFeature.CHALLENGE; // Chose your feature option:

        if (action == ApplicationFeature.COPY_DOWNLOAD_FILES) { // Upload from PC to S3, download from s3 and then
            // delete file in the s3 bucket
            app.uploadFile(TRANSIENT_BUCKET_NAME, F1, UPLOAD_DIR, F1);
            app.downloadFile(TRANSIENT_BUCKET_NAME, F1, DOWNLOAD_DIR, F1);
            app.deleteFile(TRANSIENT_BUCKET_NAME, F1);
        } else if (action == ApplicationFeature.COPY_S3_TO_S3_AND_LIST) { // Upload from s3 to s3 and list files
            app.copyFilesFromSourceS3toDestS3(PRIMARY_BUCKET_NAME, TRANSIENT_BUCKET_NAME, F3, F3);
            app.listFiles(TRANSIENT_BUCKET_NAME);
        } else if (action == ApplicationFeature.BLOCK_PUBLIC_ACCESS) {
            app.blockAllPublicAccess(PRIMARY_BUCKET_NAME);
        } else if (action == ApplicationFeature.CREATE_PRE_SIGN_URL) {
            app.uploadFile(PRIMARY_BUCKET_NAME, F1, UPLOAD_DIR, F1);
            S3Presigner s3Presigner = createS3Presigner();
            app.setS3Presigner(s3Presigner);
            var preSignUrl = app.createPreSignUrl(PRIMARY_BUCKET_NAME, F1);
            LOGGER.info("Created file `{}` pre-signed URL: `{}`", F1, preSignUrl);
        } else if (action == ApplicationFeature.DELETE_S3_BUCKET) {
            app.deleteS3Bucket(TRANSIENT_BUCKET_NAME);
        } else if (action == ApplicationFeature.CHALLENGE) {

            String tempBucketName = "temp-bucket-%s".formatted(UUID.randomUUID().toString().substring(10));
            String tempFileKey = "temp-file-key.txt";
            app.createBucket(tempBucketName);
            app.uploadFile(tempBucketName, F1, UPLOAD_DIR, tempFileKey);
            app.uploadFile(PRIMARY_BUCKET_NAME, F2, UPLOAD_DIR, F2);

            app.copyFilesFromSourceS3toDestS3(PRIMARY_BUCKET_NAME, tempBucketName, F2, F2);
            app.listFiles(tempBucketName);
            app.downloadFile(tempBucketName, tempFileKey, DOWNLOAD_DIR, tempFileKey);

            // This block is just for practicing S3 bucket actions. The link will not work because the bucket is
            // deleted in the last step.
            app.setS3Presigner(createS3Presigner());
            var url = app.createPreSignUrl(tempBucketName, tempFileKey);
            LOGGER.info("Created file `{}` pre-signed URL: `{}`", tempFileKey, url);

            List.of(tempFileKey, F2).forEach(key -> app.deleteFile(tempBucketName, key));
            app.deleteS3Bucket(tempBucketName);
        }
        LOGGER.info(System.lineSeparator() + "=".repeat(20));
    }
}
