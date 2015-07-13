package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;

public class DownloadKmsSSEFile {

	public static void DownloadFile(String bucketName, String key, File file)
			throws IOException, AmazonServiceException, AmazonClientException, InterruptedException {

		// Get the object
		AmazonS3Client s3 = new AmazonS3Client();
		
		S3Object encryptedObject = s3.getObject(new GetObjectRequest(
				bucketName, key));
		s3.setSignerRegionOverride("AWSS3V4SignerType");

		// Get the object metadata : file length and KMS encryption key
		ObjectMetadata metadata = encryptedObject.getObjectMetadata();
		Map<String, String> userMetadata = metadata.getUserMetadata();
		String kmsEncKeyName = userMetadata.get("kms-enc-key");

		// Get the Kms Key
		String kmskey = KmsUtils.getKmsKeyId(kmsEncKeyName);

		// Create the S3 Client
		AmazonS3Client s3Client = new AmazonS3EncryptionClient(
				KmsClient.getKmsClient(),
				new DefaultAWSCredentialsProviderChain(),
				new KMSEncryptionMaterialsProvider(kmskey),
				new ClientConfiguration(), new CryptoConfiguration(), null);
		s3Client.setSignerRegionOverride("AWSS3V4SignerType");

		// Proceed to the transfer
		TransferManager tx = new TransferManager(s3Client);
		
		Download mydownload = tx.download(bucketName, key, file);
		mydownload.waitForCompletion();
		tx.shutdownNow();

		s3Client.shutdown();
		s3.shutdown();
		
	}
}