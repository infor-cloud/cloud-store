package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ObjectMetadata;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


public class RenameCommand extends Command
{
  private CloudStoreClient _client;
  private RenameOptions _options;
  private String _acl;


  public RenameCommand(RenameOptions options)
  {
    _options = options;
    _client = _options.getCloudStoreClient();
    _acl = _options.getCannedAcl();
  }


  public ListenableFuture<S3File> run()
  {
    if(_options.isDryRun())
    {
      System.out.println("<DRYRUN> renaming '"
        + getSourceUri()
        + "' to '"
        + getDestUri()
        + "'");
      return Futures.immediateFuture(null);
    }

    return Futures.withFallback(buildFutureChain(), cleanupOnError());
  }


  // try to clean up if a failure occurs.  just have to worry
  // about failure during a delete phase and remove the copied
  // file
  private FutureFallback<S3File> cleanupOnError()
  {
    return new FutureFallback<S3File>()
    {
      public ListenableFuture<S3File> create(Throwable t)
      {
        DeleteOptions deleteOpts = new DeleteOptionsBuilder()
          .setCloudStoreClient(_options.getCloudStoreClient())
          .setBucketName(_options.getDestinationBucketName())
          .setObjectKey(_options.getDestinationObjectKey())
          .setForceDelete(true)
          .setIgnoreAbortInjection(true)
          .createDeleteOptions();
        ListenableFuture<S3File> deleteFuture = _client.delete(deleteOpts);

        return Futures.transform(
          deleteFuture,

          new AsyncFunction<S3File,S3File>()
          {
            public ListenableFuture<S3File> apply(S3File f)
            {
              return Futures.immediateFailedFuture(t);
            }
          });
      }
    };
  }


  // start by checking that source exists, then follow with dest check
  private ListenableFuture<S3File> buildFutureChain()
  {
    ListenableFuture<ObjectMetadata> sourceExists = 
      _client.exists(_options.getSourceBucketName(), _options.getSourceObjectKey());

    return Futures.transform(
      sourceExists,
      new AsyncFunction<ObjectMetadata,S3File>()
      {
        public ListenableFuture<S3File> apply(ObjectMetadata mdata)
          throws UsageException
        {
          if(null == mdata)
            throw new UsageException("Source object '" + getSourceUri() + "' does not exist");
          return checkDestExists();
        }
      });
  }
  

  // follow dest check with copy op
  private ListenableFuture<S3File> checkDestExists()
  {
    ListenableFuture<ObjectMetadata> destExists = 
      _client.exists(_options.getDestinationBucketName(), getDestKey());
    return Futures.transform(
      destExists,
      new AsyncFunction<ObjectMetadata,S3File>()
      {
        public ListenableFuture<S3File> apply(ObjectMetadata mdata)
          throws UsageException
        {
          if(null != mdata)
          {
            throw new UsageException("Cannot overwrite existing destination object '"
              + getDestUri());
          }
          return copyObject();
        }
      });
  }


  // copy is followed by delete
  private ListenableFuture<S3File> copyObject()
  {
    return Futures.transform(
      getCopyOp(),
      new AsyncFunction<S3File,S3File>()
      {
        public ListenableFuture<S3File> apply(S3File srcFile)
        {
          return deleteObject();
        }
      }
    );

  }


  // delete is followed by return of the dest file
  private ListenableFuture<S3File> deleteObject()
  {
    return Futures.transform(
      getDeleteOp(),
      new Function<S3File, S3File>()
      {
        public S3File apply(S3File deletedFile)
        {
          return new S3File(
            _options.getDestinationBucketName(), getDestKey());
        }
      }
    );
  }


  private ListenableFuture<S3File> getDeleteOp()
  {
    DeleteOptions deleteOpts = new DeleteOptionsBuilder()
      .setCloudStoreClient(_options.getCloudStoreClient())
      .setBucketName(_options.getSourceBucketName())
      .setObjectKey(_options.getSourceObjectKey())
      .createDeleteOptions();

    return _client.delete(deleteOpts);
  }


  private ListenableFuture<S3File> getCopyOp()
  {
    CopyOptions copyOpts = new CopyOptionsBuilder()
      .setCloudStoreClient(_options.getCloudStoreClient())
      .setSourceBucketName(_options.getSourceBucketName())
      .setSourceObjectKey(_options.getSourceObjectKey())
      .setDestinationBucketName(_options.getDestinationBucketName())
      .setDestinationObjectKey(getDestKey())
      .setCannedAcl(_acl)
      .createCopyOptions();

    return _client.copy(copyOpts);
  }


  private String getDestKey()
  {
    String key = _options.getDestinationObjectKey();
    if(key.endsWith("/"))
    {
      // moving a file into a folder....
      String src = _options.getSourceObjectKey();
      int idx = src.lastIndexOf("/");
      if(-1 != idx)
        key = key + src.substring(idx + 1);
    }
    return key;
  }


  private String getSourceUri()
  {
    return getUri(_options.getSourceBucketName(), _options.getSourceObjectKey());
  }


  private String getDestUri()
  {
    return getUri(_options.getDestinationBucketName(), getDestKey());
  }

}
