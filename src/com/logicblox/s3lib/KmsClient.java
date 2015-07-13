package com.logicblox.s3lib;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kms.AWSKMSClient;

public class KmsClient {

	// private static final String AWS_ACCESS_KEY_ID =
	// getCreds().getAWSAccessKeyId();
	// private static final String AWS_SECRET_KEY =
	// getCreds().getAWSSecretKey();

	private static AWSCredentials getCreds() {
		DefaultAWSCredentialsProviderChain _credentials = new DefaultAWSCredentialsProviderChain();
		return _credentials.getCredentials();
	}

	private static ClientConfiguration getConfig() {
		// ClientConfiguration Cfg = setConfig();
		// return Cfg;
		ClientConfiguration Cfg = new ClientConfiguration();
		return Cfg;
	}

	private static ClientConfiguration setConfig() {
		ClientConfiguration Cfg = new ClientConfiguration();
		Cfg.setProtocol(getProtocol());
		Cfg.setSocketTimeout(getSocketTimeout());
		Cfg.setConnectionTimeout(getConnectionTimeout());
		Cfg.setSignerOverride("AWSS3V4SignerType");
		return Cfg;
	}

	// Information that have to be catched from the config file.
	private static Protocol getProtocol() {
		ClientConfiguration Cfg = getConfig();
		return Cfg.getProtocol();
	}

	private static int getSocketTimeout() {
		ClientConfiguration Cfg = getConfig();
		return Cfg.getSocketTimeout();
	}

	private static int getConnectionTimeout() {
		ClientConfiguration Cfg = getConfig();
		return Cfg.getConnectionTimeout();
	}

	public static AWSKMSClient getKmsClient() {
		AWSKMSClient kmsClient = new AWSKMSClient(getCreds(), getConfig());
	/*	kmsClient.setRegion(RegionUtils.getRegion("us-east-1"));
		kmsClient.setRegion(RegionUtils.getRegion("us-west-2"));
		kmsClient.setRegion(RegionUtils.getRegion("us-west-1"));
		kmsClient.setRegion(RegionUtils.getRegion("eu-west-1"));
		kmsClient.setRegion(RegionUtils.getRegion("eu-central-1"));
		kmsClient.setRegion(RegionUtils.getRegion("ap-southeast-1"));
		kmsClient.setRegion(RegionUtils.getRegion("ap-southeast-2"));
		kmsClient.setRegion(RegionUtils.getRegion("ap-northeast-1"));
		kmsClient.setRegion(RegionUtils.getRegion("sa-east-1"));		   */
		return kmsClient;
	}
}
