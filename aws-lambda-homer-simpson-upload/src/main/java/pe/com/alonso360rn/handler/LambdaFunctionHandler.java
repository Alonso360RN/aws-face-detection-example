package pe.com.alonso360rn.handler;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.Gson;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	private static final String HOMER_SIMPSON_UPLOAD_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/152405606140/HomerSimpsonUploadQueue";

	private AmazonSQS sqs = AmazonSQSClientBuilder.standard().build();

	public LambdaFunctionHandler() {
	}

	@Override
	public String handleRequest(S3Event event, Context context) {
		String bucketName = event.getRecords().get(0).getS3().getBucket().getName();
		String objectKey = event.getRecords().get(0).getS3().getObject().getKey();

		Map<String, String> message = new HashMap<>();
		message.put("bucketName", bucketName);
		message.put("objectKey", objectKey);

		Gson gson = new Gson();

		String jsonMessage = gson.toJson(message);

		SendMessageRequest sendMessageRequest = new SendMessageRequest().withQueueUrl(HOMER_SIMPSON_UPLOAD_QUEUE_URL)
				.withMessageBody(jsonMessage);

		sqs.sendMessage(sendMessageRequest);

		return jsonMessage;
	}
}