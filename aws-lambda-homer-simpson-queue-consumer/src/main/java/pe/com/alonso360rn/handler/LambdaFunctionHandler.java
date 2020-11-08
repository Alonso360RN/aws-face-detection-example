package pe.com.alonso360rn.handler;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.rekognition.model.SearchFacesByImageRequest;
import com.amazonaws.services.rekognition.model.SearchFacesByImageResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.Gson;

public class LambdaFunctionHandler implements RequestHandler<SQSEvent, String> {

	private static final String COLLECTION_ID = "HomerSimpsonCollection";

	private static final String HOMER_SIMPSON_MATCH_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/152405606140/HomerSimpsonMatchQueue";

	private AmazonRekognition rekognition = AmazonRekognitionClientBuilder.standard().build();

	private AmazonSQS sqs = AmazonSQSClientBuilder.standard().build();

	@Override
	public String handleRequest(SQSEvent event, Context context) {
		Gson gson = new Gson();
		String answer = "";

		for (SQSMessage message : event.getRecords()) {
			Map<String, String> parsedMessage = gson.fromJson(message.getBody(), Map.class);

			String bucketName = parsedMessage.get("bucketName");
			String objectKey = parsedMessage.get("objectKey");

			Image image = new Image().withS3Object(new S3Object().withBucket(bucketName).withName(objectKey));

			SearchFacesByImageRequest searchFacesByImageRequest = new SearchFacesByImageRequest()
					.withCollectionId(COLLECTION_ID).withImage(image).withFaceMatchThreshold(80F).withMaxFaces(2);

			Map<String, Object> result = new HashMap<>();
			result.put("bucketName", bucketName);
			result.put("objectKey", objectKey);

			SearchFacesByImageResult searchFacesByImageResult = rekognition
					.searchFacesByImage(searchFacesByImageRequest);

			if (searchFacesByImageResult.getFaceMatches().isEmpty()) {
				result.put("isHomerSimpsonPicture", false);

				answer = "This is not a Homer Simpson picture";
			} else {
				result.put("isHomerSimpsonPicture", true);

				answer = "This is a Homer Simpson picture";
			}
			
			SendMessageRequest sendMessageRequest = new SendMessageRequest()
					.withQueueUrl(HOMER_SIMPSON_MATCH_QUEUE_URL).withMessageBody(gson.toJson(result));

			sqs.sendMessage(sendMessageRequest);
		}

		return answer;
	}

}
