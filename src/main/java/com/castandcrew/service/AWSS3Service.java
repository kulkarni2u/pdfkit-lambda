package com.castandcrew.service;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

public class AWSS3Service {
    private final AmazonS3 s3Client;
    private TransferManager transferManager;

    public AWSS3Service(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    public AWSS3Service() {
        this.s3Client = AmazonS3ClientBuilder.standard().build();
        this.transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
    }

    public Map<String, String> getS3ObjectDetails(final S3Event event) {
        Map<String, String> result = new HashMap<>();
        final S3EventNotification.S3Entity s3Entity = event.getRecords().get(0).getS3();
        String bucket = s3Entity.getBucket().getName();
        String key = s3Entity.getObject().getKey();

        result.put("bucket", bucket);
        result.put("key", key);

        return result;
    }

    public S3Object getObject(final String bucket, final String key) {
        return s3Client.getObject(new GetObjectRequest(bucket, key));
    }

    public S3Object getObject(final S3Event event) {
        Map<String, String> objectDetails = this.getS3ObjectDetails(event);
        return getObject(objectDetails.get("bucket"), objectDetails.get("key"));
    }

    public boolean doesBucketExists(String bucketName) {
        return s3Client.doesBucketExistV2(bucketName);
    }

    //deleting an object
    public void deleteObject(String bucketName, String objectKey) {
        s3Client.deleteObject(bucketName, objectKey);
    }

    public void transerFile(final ByteArrayInputStream inputStream, final ObjectMetadata metadata, final String targetBucket, final String key) {
        System.out.println("PDFKit - Transfer starting");
        try {
            Upload upload = transferManager.upload(targetBucket, key, inputStream, metadata);
            upload.waitForUploadResult();
            inputStream.close();
            transferManager.shutdownNow(false);
            System.out.println("Transfer Complete!");
        } catch (Exception e) {
            System.out.println("Transfer Failed!" + e.getMessage());
        }
    }
}
