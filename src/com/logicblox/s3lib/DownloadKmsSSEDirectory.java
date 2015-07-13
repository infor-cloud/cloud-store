package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class DownloadKmsSSEDirectory {
	public static void downloadDirectory(String bucketName, String key,
			File directory) throws IOException, AmazonServiceException,
			AmazonClientException, InterruptedException {

		AmazonS3Client s3Client = new AmazonS3Client();
		s3Client.setSignerRegionOverride("AWSS3V4SignerType");
		List<String> objects = new ArrayList<String>();
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(bucketName).withPrefix(key);
		ObjectListing objectListing;

		do {
			objectListing = s3Client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing
					.getObjectSummaries()) {
				objects.add(objectSummary.getKey());
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());

		if (!directory.exists()) {
			if (directory.mkdir()) {
				System.out.println("Directory " + directory + " is created!");
				for (String item : objects) {
					String desdir = item.substring(0, item.lastIndexOf('/')) + '/';
					String path = directory + File.separator + desdir;
					File destination = new File(path);
					if (destination.mkdirs()) {
						System.out.println("Directory " + destination
								+ " is created!");
						File f = new File(directory + "/" + item);
						DownloadKmsSSEFile.DownloadFile(bucketName, item, f);
						System.out.println("Downloading the file " + f);
					} else {
						System.out.println("Failed to create " + destination);
					}
				}

			} else {
				System.out.println("Failed to create directory!" + directory);
			}
		} else {
			System.out.println("Directory" + directory
					+ " already exists, adding/overwriting files!");
			for (String item : objects) {
				String desdir = item.substring(0, item.lastIndexOf('/')) + '/';
				String path = directory + File.separator + desdir;
				File destination = new File(path);
				if (destination.mkdirs()) {
					System.out.println("Directory " + destination
							+ " is created!");
					File f = new File(directory + "/" + item);
					DownloadKmsSSEFile.DownloadFile(bucketName, item, f);
					System.out.println("Downloading the file " + f);
				} else {
					System.out.println("Directory " + destination
							+ " already exists, adding/overwriting files!");
					File f = new File(directory + "/" + item);
					DownloadKmsSSEFile.DownloadFile(bucketName, item, f);
					System.out.println("Downloading the file " + f);
				}
			}
		}
	}
}
