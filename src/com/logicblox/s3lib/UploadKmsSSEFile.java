package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class UploadKmsSSEFile {
	private static String kmsEncKey;

	public static void uploadFile(File file, String kmsEncKeyName, String acl,
			String bucketName, String key) throws IOException {

		try {
			kmsEncKey = KmsUtils.getKmsKeyId(kmsEncKeyName);
		} catch (com.amazonaws.AmazonServiceException e) {
			throw new RuntimeException(e);
		}

		// if a KMS key matchs the specified KMS KeyName, perform multi-part
		// upload and ask for server-side encryption
		if (kmsEncKey != null) {
			
			AmazonS3Client s3Client = new AmazonS3Client();
			s3Client.setSignerRegionOverride("AWSS3V4SignerType");
			
			// Create a list of UploadPartResponse objects. You get one of these
			// for each part upload.
			List<PartETag> partETags = new ArrayList<PartETag>();

			// Set Object Meta-data
			Map<String, String> userMetadata = new HashMap();
			userMetadata.put("s3tool-version", String.valueOf(Version.CURRENT));
			userMetadata.put("s3tool-file-length", Long.toString(file.length()));
			userMetadata.put("kms-enc-key", kmsEncKey);

			ObjectMetadata objectMetadata = new ObjectMetadata();
			objectMetadata.setUserMetadata(userMetadata);

			// Step 1: Initialize.
			InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
					bucketName, key).withSSEAwsKeyManagementParams(
					new SSEAwsKeyManagementParams(kmsEncKey))
					.withObjectMetadata(objectMetadata);
			InitiateMultipartUploadResult initResponse = s3Client
					.initiateMultipartUpload(initRequest);
			long contentLength = file.length();
			long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.

			try {
				// Step 2: Upload parts.
				long filePosition = 0;
				for (int i = 1; filePosition < contentLength; i++) {
					// Last part can be less than 5 MB. Adjust part size.
					partSize = Math.min(partSize,
							(contentLength - filePosition));

					// Create request to upload a part.
					UploadPartRequest uploadRequest = new UploadPartRequest()
							.withBucketName(bucketName).withKey(key)
							.withUploadId(initResponse.getUploadId())
							.withPartNumber(i).withFileOffset(filePosition)
							.withFile(file).withPartSize(partSize);

					// Upload part and add response to our list.
					partETags.add(s3Client.uploadPart(uploadRequest)
							.getPartETag());

					filePosition += partSize;
				}

				// Step 3: Complete.
				CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
						bucketName, key, initResponse.getUploadId(), partETags);

				s3Client.completeMultipartUpload(compRequest);

				s3Client.shutdown();

			} catch (Exception e) {
				s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
						bucketName, key, initResponse.getUploadId()));
			}

		}

	}
}
