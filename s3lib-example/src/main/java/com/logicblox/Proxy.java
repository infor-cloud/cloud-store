package com.logicblox;

import com.logicblox.s3lib.*;

/**
 * Proxy
 *
 */
public class Proxy
{
    public static void main( String[] args )
    {
      ListeningExecutorService uploadExecutor = getHttpExecutor();
      ListeningScheduledExecutorService internalExecutor = getInternalExecutor();

      ClientConfiguration clientCfg = new ClientConfiguration();
      clientCfg.setProtocol(Protocol.HTTPS);

      //setup proxy connection:
      clientCfg.setProxyHost("localhost");
      clientCfg.setProxyPort(8118);

      AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

      S3Client client = new S3Client(
          s3Client,
          uploadExecutor,
          internalExecutor,
          chunkSize,
          getKeyProvider());
    }
}
