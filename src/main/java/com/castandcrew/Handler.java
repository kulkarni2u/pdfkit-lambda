package com.castandcrew;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.castandcrew.service.AWSS3Service;
import com.castandcrew.service.PdfService;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.util.Collections;

public class Handler implements RequestHandler<S3Event, ApiGatewayResponse> {
	private static final Logger LOG = Logger.getLogger(Handler.class);
	private AmazonS3 s3;
	private AWSS3Service s3Service;
	private PdfService pdfService;

	public Handler() {
		System.out.println("In Handler?");
	}

	// For testing only
	public Handler(AmazonS3 s3) { this.s3 = s3; }

	@Override
	public ApiGatewayResponse handleRequest(S3Event event, Context context) {
		BasicConfigurator.configure();
		this.initAWSService();
		context.getLogger().log("Received S3Event " + event);
		Response responseBody;
		try {
			// Read the input.
			final S3Object resp = s3Service.getObject(event);
			final ObjectMetadata objectMetadata = resp.getObjectMetadata();
			final String destBucket = "pdfkit-target";
			final String contentType = objectMetadata.getContentType();
			// Move the file to destination bucket
			context.getLogger().log("Moving to destination bucket: " + destBucket);
			pdfService.putObject(resp, destBucket);
			context.getLogger().log("Move Successful to: " + destBucket);

			responseBody = new Response("Execution Successful, moved to: " + destBucket, contentType);
		} catch (Exception e) {
			LOG.error("Process failed", e);
			responseBody = new Response("Process Failed: ", e.getMessage());
		}

		return ApiGatewayResponse.builder()
				.setStatusCode(200)
				.setObjectBody(responseBody)
				.setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda and Cast & Crew"))
				.build();
	}

	private void initAWSService() {
		s3Service = s3 == null ? new AWSS3Service() : new AWSS3Service(s3);
		pdfService = new PdfService(s3Service);

	}
}
