package com.castandcrew;

import java.util.Collections;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.castandcrew.service.AWSS3Service;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Handler implements RequestHandler<S3Event, ApiGatewayResponse> {
	private static final Logger LOG = Logger.getLogger(Handler.class);
	private AmazonS3 s3;
	private AWSS3Service s3Service;

	public Handler() {
		System.out.println("In Handler?");
	}

	// For testing only
	public Handler(AmazonS3 s3) { this.s3 = s3; }

	@Override
	public ApiGatewayResponse handleRequest(S3Event event, Context context) {
		BasicConfigurator.configure();
		System.out.println("Coming Here?");
		this.initAWSService();
		LOG.info("received: " + event);
		context.getLogger().log("Received S3Event " + event);
		try {
			// Read the input.
			final S3Object resp = s3Service.getObject(event);
			final ObjectMetadata objectMetadata = resp.getObjectMetadata();
			final String destBucket = resp.getBucketName().replaceAll("bucket", "target");
			final String destKey = "target-" + resp.getKey();
			final String contentType = objectMetadata.getContentType();
			// Move the file to destination bucket
			context.getLogger().log("Moving to destination bucket: " + destBucket);
			putObject(resp, destBucket, destKey);
			context.getLogger().log("Move Successful to: " + destBucket);

			Response responseBody = new Response("Go Serverless v1.x! Function executed successfully!", contentType);
			return ApiGatewayResponse.builder()
					.setStatusCode(200)
					.setObjectBody(responseBody)
					.setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda and Cast & Crew"))
					.build();
		} catch (Exception e) {
			context.getLogger().log("Error reading the object");
			throw e;
		}
	}
	// TODO: Move this logic to PDFService
	private void putObject(S3Object resp, String bucket, String key) {
		s3Service.putObject(resp.getObjectContent(), resp.getObjectMetadata(), bucket, key);
//		s3Service.deleteObject(resp.getBucketName(), resp.getKey());
	}

	private void initAWSService() {
		if (s3Service == null) {
			s3Service = s3 == null ? new AWSS3Service() : new AWSS3Service(s3);
		}
	}
}
