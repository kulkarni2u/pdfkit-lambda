package com.castandcrew;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Handler implements RequestHandler<S3Event, ApiGatewayResponse> {
	private static final Logger LOG = Logger.getLogger(Handler.class);
	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

	public Handler() {
	}

	// For testing only
	public Handler(AmazonS3 s3) {
		this.s3 = s3;
	}

	@Override
	public ApiGatewayResponse handleRequest(S3Event event, Context context) {
		BasicConfigurator.configure();

		LOG.info("received: " + event);
		context.getLogger().log("Received S3Event " + event);
		final S3EventNotification.S3Entity s3Entity = event.getRecords().get(0).getS3();
		String bucket = s3Entity.getBucket().getName();
		String key = s3Entity.getObject().getKey();
		final String destBucket = bucket.replaceAll("bucket", "target");
		final String destKey = "target-" + key;
		final Map<String, Object> input = new HashMap<>();

		try {
			// Read the input.
			S3Object resp = s3.getObject(new GetObjectRequest(bucket, key));
			String contentType = resp.getObjectMetadata().getContentType();
			input.put("contentType", contentType);

			context.getLogger().log("Content Type: " + contentType);
			// Move the file to destination bucket
			context.getLogger().log("Moving to destination bucket: " + destBucket);
			InputStream inputStream = resp.getObjectContent();
			ObjectMetadata meta = resp.getObjectMetadata();
			s3.putObject(destBucket, destKey, inputStream, meta);
			context.getLogger().log("Move Success " + destBucket);
		} catch (Exception e) {
			context.getLogger().log(String.format("Error reading the object %s from %s", key, bucket));
			throw e;
		}

		Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", input);
		return ApiGatewayResponse.builder()
				.setStatusCode(200)
				.setObjectBody(responseBody)
				.setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda and Cast & Crew"))
				.build();
	}
}
