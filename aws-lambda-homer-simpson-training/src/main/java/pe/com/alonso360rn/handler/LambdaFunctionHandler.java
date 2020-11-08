package pe.com.alonso360rn.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.IndexFacesRequest;
import com.amazonaws.services.rekognition.model.IndexFacesResult;
import com.amazonaws.services.rekognition.model.S3Object;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	private AmazonRekognition rekognition = AmazonRekognitionClientBuilder.defaultClient();

	private static final String HOMER_SIMPSON_COLLECTION = "HomerSimpsonCollection";

	@Override
	public String handleRequest(S3Event event, Context context) {
		String bucketName = event.getRecords().get(0).getS3().getBucket().getName();
		String objectKey = event.getRecords().get(0).getS3().getObject().getKey();

		Image image = new Image().withS3Object(new S3Object().withBucket(bucketName).withName(objectKey));

		IndexFacesRequest indexFacesRequest = new IndexFacesRequest().withImage(image)
				.withCollectionId(HOMER_SIMPSON_COLLECTION).withExternalImageId(objectKey)
				.withDetectionAttributes("DEFAULT");
		
		IndexFacesResult indexFacesResult = rekognition.indexFaces(indexFacesRequest);
		
		if (indexFacesResult.getFaceRecords().isEmpty()) {
			context.getLogger().log("Face not found");
			
			return "Could not find a face";
		} else {
			String faceId = indexFacesResult.getFaceRecords().get(0).getFace().getFaceId();
			String location = indexFacesResult.getFaceRecords().get(0).getFace().getBoundingBox().toString();
			
			context.getLogger().log(faceId.concat(" -> ").concat(location));
			
			return faceId.concat(" -> ").concat(location);
		}
	}
}