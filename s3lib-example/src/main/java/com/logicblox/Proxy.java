package com.logicblox;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.File;
import java.util.concurrent.Executors;

/**
 * Proxy
 */
public class Proxy {
  private long chunkSize = Utils.getDefaultChunkSize();

  private static ListeningExecutorService getHttpExecutor() {
    int maxConcurrentConnections = 10;
    return MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(maxConcurrentConnections));
  }

  private static ListeningScheduledExecutorService getInternalExecutor() {
    return MoreExecutors.listeningDecorator(
        Executors.newScheduledThreadPool(50));
  }

  private static KeyProvider getKeyProvider() {
    String encKeyDirectory = Utils.getDefaultKeyDirectory();
    File dir = new File(encKeyDirectory);
    if (!dir.exists() && !dir.mkdirs())
      throw new UsageException("specified key directory '" + encKeyDirectory + "' does not exist");

    if (!dir.isDirectory())
      throw new UsageException("specified key directory '" + encKeyDirectory + "' is not a directory");

    return new DirectoryKeyProvider(dir);
  }

  public static void main(String[] args) {
    ClientConfiguration clientCfg = new ClientConfiguration();
    clientCfg.setProtocol(Protocol.HTTPS);

    //setup proxy connection:
    clientCfg.setProxyHost("localhost");
    clientCfg.setProxyPort(8118);

    AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

    long chunkSize = Utils.getDefaultChunkSize();

    S3Client client = new S3Client(
        s3Client,
        getHttpExecutor(),
        getInternalExecutor(),
        chunkSize,
        getKeyProvider());
  }
}
