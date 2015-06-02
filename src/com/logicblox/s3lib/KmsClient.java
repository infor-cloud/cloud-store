package com.logicblox.s3lib;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
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
		return new ClientConfiguration();
	}

	private static ClientConfiguration setConfig() {
		ClientConfiguration Cfg = new ClientConfiguration();
		Cfg.setProtocol(getProtocol());
		Cfg.setSocketTimeout(getSocketTimeout());
		Cfg.setConnectionTimeout(getConnectionTimeout());
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
		return kmsClient;
	}
}
