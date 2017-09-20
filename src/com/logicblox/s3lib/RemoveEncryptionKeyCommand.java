package com.logicblox.s3lib;


import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class RemoveEncryptionKeyCommand extends Command
{
  private EncryptionKeyOptions _options;
  private String _encKeyName;
  private KeyProvider _encKeyProvider;

  public RemoveEncryptionKeyCommand(EncryptionKeyOptions options)
  throws IOException
  {
    super(options);
    _options = options;
    _encKeyProvider = _client.getKeyProvider();
    _encKeyName = _options.getEncryptionKey();
  }

  public ListenableFuture<StoreFile> run()
  {
    // TODO(geokollias): Handle versions?
    ListenableFuture<S3ObjectMetadata> objMeta = getMetadata();
    objMeta = Futures.transform(objMeta, removeEncryptionKeyFn());
    ListenableFuture<StoreFile> res = Futures.transform(objMeta,
      updateObjectMetadataFn());

    return Futures.withFallback(
      res,
      new FutureFallback<StoreFile>()
      {
        public ListenableFuture<StoreFile> create(Throwable t)
        {
          if (t instanceof UsageException) {
            return Futures.immediateFailedFuture(t);
          }
          return Futures.immediateFailedFuture(new Exception("Error " +
                                                             "adding new encryption key to " +
                                                             getUri(_options.getBucketName(), _options.getObjectKey()) + ".", t));
        }
      });
  }

  /**
   * Step 1: Fetch metadata.
   */
  private ListenableFuture<S3ObjectMetadata> getMetadata()
  {
    return executeWithRetry(
      _client.getInternalExecutor(),
      new Callable<ListenableFuture<S3ObjectMetadata>>()
      {
        public ListenableFuture<S3ObjectMetadata> call()
        {
          return getMetadataAsync();
        }

        public String toString()
        {
          return "Starting removal of existing encryption key to " +
                 getUri(_options.getBucketName(), _options.getObjectKey());
        }
      });
  }

  private ListenableFuture<S3ObjectMetadata> getMetadataAsync()
  {
    S3ObjectMetadataFactory f = new S3ObjectMetadataFactory(getAmazonS3Client(),
      _client.getApiExecutor());
    ListenableFuture<S3ObjectMetadata> metadataFactory = f.create(
      _options.getBucketName(), _options.getObjectKey(), null);

    AsyncFunction<S3ObjectMetadata, S3ObjectMetadata> checkMetadata = new
      AsyncFunction<S3ObjectMetadata, S3ObjectMetadata>()
      {
        public ListenableFuture<S3ObjectMetadata> apply(S3ObjectMetadata metadata)
        {
          Map<String, String> userMetadata = metadata.getUserMetadata();
          String obj = getUri(metadata.getBucket(), metadata.getKey());
          if (!userMetadata.containsKey("s3tool-key-name"))
          {
            throw new UsageException("Object doesn't seem to be encrypted");
          }
           if (!userMetadata.containsKey("s3tool-pubkey-hash"))
          {
            throw new UsageException("Public key hashes are required when " +
                                     "object has multiple encryption keys");
          }
          return Futures.immediateFuture(metadata);
        }
      };

    return Futures.transform(metadataFactory, checkMetadata);
  }

  /**
   * Step 2: Remove encryption key
   */
  private AsyncFunction<S3ObjectMetadata, S3ObjectMetadata> removeEncryptionKeyFn()
  {
    return new AsyncFunction<S3ObjectMetadata, S3ObjectMetadata>()
    {
      public ListenableFuture<S3ObjectMetadata> apply(final S3ObjectMetadata metadata)
      {
        Callable removeEncyptionKeyCall = new Callable<S3ObjectMetadata>()
        {
          public S3ObjectMetadata call()
          {
            return removeEncryptionKey(metadata);
          }
        };

        return _client.getInternalExecutor().submit(removeEncyptionKeyCall);
      }
    };
  }

  private S3ObjectMetadata removeEncryptionKey(S3ObjectMetadata metadata)
  {
    String errPrefix = getUri(metadata.getBucket(), metadata.getKey()) + ": ";
    if (_encKeyProvider == null)
    {
      throw new UsageException(errPrefix + "No encryption key provider is " +
                               "specified");
    }
    if (_encKeyName == null)
    {
      throw new UsageException(errPrefix + "No encryption key name is " +
                                 "specified");
    }
    Map<String, String> userMetadata = metadata.getUserMetadata();
    String keyNamesStr = userMetadata.get("s3tool-key-name");
    List<String> keyNames;
    if(null == keyNamesStr)
      keyNames = new ArrayList<String>();
    else
      keyNames = new ArrayList<>(Arrays.asList(keyNamesStr.split(",")));

    if (keyNames.size() == 1)
    {
      throw new UsageException(errPrefix + "Cannot remove the last remaining " +
                               "key.");
    }

    int removedKeyIndex = keyNames.indexOf(_encKeyName);
    if (removedKeyIndex == -1)
    {
      throw new UsageException(errPrefix + "Encryption key " +
                               _encKeyName + " doesn't exist");
    }

    // Update user-metadata
    ObjectMetadata allMeta = metadata.getAllMetadata();

    keyNames.remove(removedKeyIndex);
    allMeta.addUserMetadata("s3tool-key-name", Joiner.on(",").join(keyNames));

    String symKeysStr = userMetadata.get("s3tool-symmetric-key");
    List<String> symKeys = new ArrayList<>(Arrays.asList(
      symKeysStr.split(",")));
    symKeys.remove(removedKeyIndex);
    allMeta.addUserMetadata("s3tool-symmetric-key",
      Joiner.on(",").join(symKeys));

    String pubKeyHashesStr = userMetadata.get("s3tool-pubkey-hash");
    List<String> pubKeyHashes = new ArrayList<>(Arrays.asList(
      pubKeyHashesStr.split(",")));
    pubKeyHashes.remove(removedKeyIndex);
    allMeta.addUserMetadata("s3tool-pubkey-hash",
      Joiner.on(",").join(pubKeyHashes));

    return metadata;
  }

  /**
   * Step 3: Update object's metadata
   */
  private AsyncFunction<S3ObjectMetadata, StoreFile> updateObjectMetadataFn()
  {
    AsyncFunction<S3ObjectMetadata, StoreFile> update = new
      AsyncFunction<S3ObjectMetadata, StoreFile>()
      {
        public ListenableFuture<StoreFile> apply(S3ObjectMetadata metadata)
        throws IOException
        {
          if(null == getGCSClient())
          {
            // It seems in AWS there is no way to update an object's metadata
            // without re-uploading/copying the whole object. Here, we
            // copy the object to itself in order to add the new
            // user-metadata.
            CopyOptions options = _client.getOptionsBuilderFactory()
              .newCopyOptionsBuilder()
              .setSourceBucketName(metadata.getBucket())
              .setSourceObjectKey(metadata.getKey())
              .setDestinationBucketName(metadata.getBucket())
              .setDestinationObjectKey(metadata.getKey())
              .setUserMetadata(metadata.getUserMetadata())
              .setKeepAcl(true)
              .createOptions();

            return _client.copy(options);
          }
          else
          {
            return executeWithRetry(
              _client.getInternalExecutor(),
              new Callable<ListenableFuture<StoreFile>>()
              {
                public ListenableFuture<StoreFile> call()
                {
                  return _client.getApiExecutor().submit(new Callable<StoreFile>()
                  {
                    public StoreFile call()
                    throws IOException
                    {
                      GCSClient.patchMetaData(getGCSClient(), metadata.getBucket(),
                        metadata.getKey(), metadata.getUserMetadata());
                      return new StoreFile(metadata.getBucket(), metadata.getKey());
                    }
                  });
                }
              });
          }
        }
      };

    return update;
  }
}
