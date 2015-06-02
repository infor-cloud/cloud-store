package com.logicblox.s3lib;

import java.util.List;

import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.*;

public class KmsUtils {

	private static AWSKMSClient kmsclient = KmsClient.getKmsClient();

	public static ListKeysResult getAllKeys() {
		ListKeysResult allKeys = kmsclient.listKeys();
		return allKeys;
	}

	public static ListAliasesResult getAllAliases() {
		ListAliasesResult allAliases = kmsclient.listAliases();
		return allAliases;
	}

	public static String getAliasNameFromKeyId(String keyId) {
		String aliasName = "";
		List<AliasListEntry> aliases = KmsUtils.getAllAliases().getAliases();
		for (AliasListEntry aliasEntry : aliases) {
			try {
				String targetKeyId = aliasEntry.getTargetKeyId();
				if (targetKeyId.equals(keyId)) {
					String alias = aliasEntry.getAliasName();
					if (alias.isEmpty()) {
						aliasName = "'None'";
					} else {
						aliasName = alias;
					}
				} else {
					aliasName = "'None'";
				}
			} catch (Exception e) {
			}
		}
		return aliasName;
	}

	public static String getKeyDescription(String keyId) {
		DescribeKeyRequest request = new DescribeKeyRequest();
		request.setKeyId(keyId);
		DescribeKeyResult response = kmsclient.describeKey(request);
		KeyMetadata data = response.getKeyMetadata();
		return data.getDescription();
	}

	public static CreateKeyResult createKey(String description,
			String keyUsage, String policy) {

		CreateKeyRequest request = new CreateKeyRequest();
		if (description != null) {
			request.setDescription(description.toString());
		}
		if (keyUsage != null) {
			request.setKeyUsage(keyUsage.toString());
		}
		if (policy != null) {
			request.setPolicy(policy.toString());
		}
		return kmsclient.createKey(request);
	}

	public CreateKeyResult createKey(String description, String keyUsage) {
		return createKey(description, keyUsage, null);
	}

	public static CreateKeyResult createKey(String description) {
		return createKey(description, null, null);
	}

	public static CreateKeyResult createKey() {
		return createKey(null, null, null);
	}
}
