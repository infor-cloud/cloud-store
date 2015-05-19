package com.logicblox.s3lib;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.*;
import com.amazonaws.util.json.JSONObject;

public class KmsUtils
{
	public static ListKeysResult getAllKeys()
	  {
		AWSKMS kmsClient  = new AWSKMSClient();
		ListKeysResult allKeys = kmsClient.listKeys();
	    return allKeys ;
	  }
	
	public static ListAliasesResult getAllAliases()
	  {
		AWSKMS kmsClient  = new AWSKMSClient();
		ListAliasesResult allAliases = kmsClient.listAliases();
		return allAliases;
      }
	
	public static String retAliasName(String aliasId)
	  {
		String aliasName = "";
		ListAliasesResult allAliases = KmsUtils.getAllAliases();
		List<AliasListEntry> aliases = allAliases.getAliases();
		int i;
		for (i=0; i < aliases.size(); i++)
			{
				AliasListEntry aliasEntry = aliases.get(i);
				try
				{
				  String targetKeyId = aliasEntry.getTargetKeyId();
				  if (targetKeyId.equals(aliasId))
				   {
					 String alias = aliasEntry.getAliasName();
					 if (alias.isEmpty())
					   {
						 aliasName = "None";
					   }
					 else
					  {
						 aliasName = alias;
					  }
				   }
				  else
				   {
					  aliasName = "'None'";
				   }
				}
				catch (Exception e) {}
				
			}
		return aliasName;
	  }
	
	public static String getKeyDescription(String keyId)
	{
		AWSKMS kmsClient  = new AWSKMSClient();
		DescribeKeyRequest request = new DescribeKeyRequest();
		request.setKeyId(keyId);
		DescribeKeyResult response = kmsClient.describeKey(request);
		KeyMetadata data = response.getKeyMetadata();
		String description = data.getDescription();
		return description;
	}

	
	
	
	public static CreateKeyResult createKey(String description , String keyUsage, String policy)
	{
		AWSKMS kmsClient  = new AWSKMSClient();
		CreateKeyRequest request = new CreateKeyRequest();
		request.setDescription(description);
		request.setKeyUsage(keyUsage);
		request.setPolicy(policy);
		CreateKeyResult kmsKey = kmsClient.createKey(request);
		return kmsKey;
	}
	
	public static CreateKeyResult createKey(String description , String keyUsage)
	{
		AWSKMS kmsClient  = new AWSKMSClient();
		CreateKeyRequest request = new CreateKeyRequest();
		request.setDescription(description);
		request.setKeyUsage(keyUsage);
		CreateKeyResult kmsKey = kmsClient.createKey(request);
		return kmsKey;
	}
	
	public static CreateKeyResult createKey(String description)
	{
		AWSKMS kmsClient  = new AWSKMSClient();
		CreateKeyRequest request = new CreateKeyRequest();
		request.setDescription(description);
		request.setKeyUsage("ENCRYPT_DECRYPT");
		CreateKeyResult kmsKey = kmsClient.createKey(request);
		return kmsKey;
	}
	
	public static CreateKeyResult createKey()
	{
		AWSKMS kmsClient  = new AWSKMSClient();
		CreateKeyRequest request = new CreateKeyRequest();
		request.setKeyUsage("ENCRYPT_DECRYPT");
		CreateKeyResult kmsKey = kmsClient.createKey(request);
		return kmsKey;
	}
	
	
	
	
	
	
}







