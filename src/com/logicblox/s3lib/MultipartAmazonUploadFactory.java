package com.logicblox.s3lib;

import java.util.Map;
import java.util.concurrent.Callable;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;

class MultipartAmazonUploadFactory implements UploadFactory
{
  private AmazonS3 client;
  private ListeningExecutorService executor;
  private S3ProgressListenerFactory progressListenerFactory;

  public MultipartAmazonUploadFactory(AmazonS3 client,
                                      ListeningExecutorService executor,
                                      S3ProgressListenerFactory progressListenerFactory)
  {
    if(client == null)
      throw new IllegalArgumentException("non-null client is required");
    if(executor == null)
      throw new IllegalArgumentException("non-null executor is required");

    this.client = client;
    this.executor = executor;
    this.progressListenerFactory = progressListenerFactory;
  }

  public ListenableFuture<Upload> startUpload(String bucketName, String key, Map<String,String> meta, String cannedAcl)
  {
    return executor.submit(new StartCallable(bucketName, key, meta, cannedAcl));
  }

  private class StartCallable implements Callable<Upload>
  {
    private String key;
    private String bucketName;
    private Map<String,String> meta;
    private String cannedAcl;

    public StartCallable(String bucketName, String key, Map<String,String> meta, String cannedAcl)
    {
      this.bucketName = bucketName;
      this.key = key;
      this.meta = meta;
      this.cannedAcl = cannedAcl;
    }

    public Upload call() throws Exception
    {
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setUserMetadata(meta);
      InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest(bucketName, key, metadata);
      req.setCannedACL(getAcl(cannedAcl));
      InitiateMultipartUploadResult res = client.initiateMultipartUpload(req);
      return new MultipartAmazonUpload(client, bucketName, key,
          res.getUploadId(), executor, progressListenerFactory);
    }

    private CannedAccessControlList getAcl(String value)
    {
      for (CannedAccessControlList acl : CannedAccessControlList.values())
      {
        if (acl.toString().equals(value))
        {
          return acl;
        }
      }
      return null;
    }
  }
}
