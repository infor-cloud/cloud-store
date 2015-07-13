package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;

public class UploadKmsSSEDirectory {
	public static void uploadDirectory(String directory, String kmsEncKeyName,
			String acl, String bucketName, String key) throws IOException {

		File[] files = new File(directory).listFiles();
		for (File file : files) {
			if (file.isFile()) {
				UploadKmsSSEFile.uploadFile(file, kmsEncKeyName, acl,
						bucketName, key + "/" + file.getName());
			}
		}

	}

}
