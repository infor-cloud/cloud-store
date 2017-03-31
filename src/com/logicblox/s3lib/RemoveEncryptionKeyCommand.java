package com.logicblox.s3lib;


import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class RemoveEncryptionKeyCommand extends Command
{
  private final Logger _logger;
  private CloudStoreClient _client;
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private String _encKeyName;
  private KeyProvider _encKeyProvider;

  public RemoveEncryptionKeyCommand(
    ListeningExecutorService httpExecutor,
    ListeningScheduledExecutorService internalExecutor,
    CloudStoreClient client,
    String encKeyName,
    KeyProvider encKeyProvider)
  throws IOException
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
    _client = client;
    _encKeyName = encKeyName;
    _encKeyProvider = encKeyProvider;

    _logger = LoggerFactory.getLogger(RemoveEncryptionKeyCommand.class);
  }

  public ListenableFuture<S3File> run(final String bucket,final String key,
                                      final String version)
  {
    // TODO(geokollias): Handle versions?
    ListenableFuture<S3ObjectMetadata> objMeta = getMetadata(bucket, key);
    objMeta = Futures.transform(objMeta, removeEncryptionKeyFn());
    ListenableFuture<S3File> res = Futures.transform(objMeta,
      updateObjectMetadataFn());

    return Futures.withFallback(
      res,
      new FutureFallback<S3File>()
      {
        public ListenableFuture<S3File> create(Throwable t)
        {
          if (t instanceof UsageException) {
            return Futures.immediateFailedFuture(t);
          }
          return Futures.immediateFailedFuture(new Exception("Error " +
              "adding new encryption key to " + getUri(bucket, key)+ ".", t));
        }
      });
  }

  /**
   * Step 1: Fetch metadata.
   */
  private ListenableFuture<S3ObjectMetadata> getMetadata(final String bucket,
                                                         final String key)
  {
    return executeWithRetry(
      _executor,
      new Callable<ListenableFuture<S3ObjectMetadata>>()
      {
        public ListenableFuture<S3ObjectMetadata> call()
        {
          return getMetadataAsync(bucket, key);
        }

        public String toString()
        {
          return "Starting removal of existing encryption key to " +
                 getUri(bucket, key);
        }
      });
  }

  private ListenableFuture<S3ObjectMetadata> getMetadataAsync(final String
                                                                bucket,
                                                              final String key)
  {
    S3ObjectMetadataFactory f = new S3ObjectMetadataFactory(getAmazonS3Client(),
      _httpExecutor);
    ListenableFuture<S3ObjectMetadata> metadataFactory = f.create(bucket, key,
      null);

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

        return _executor.submit(removeEncyptionKeyCall);
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
  private AsyncFunction<S3ObjectMetadata, S3File> updateObjectMetadataFn()
  {
    AsyncFunction<S3ObjectMetadata, S3File> update = new
      AsyncFunction<S3ObjectMetadata, S3File>()
      {
        public ListenableFuture<S3File> apply(final S3ObjectMetadata metadata)
        throws IOException
        {
          ListenableFuture<AccessControlList> acl = executeWithRetry(
            _executor,
            new Callable<ListenableFuture<AccessControlList>>()
            {
              public ListenableFuture<AccessControlList> call()
              {
                return _httpExecutor.submit(
                  new Callable<AccessControlList>()
                  {
                    public AccessControlList call()
                    {
		      return Utils.getObjectAcl(
		        getAmazonS3Client(), metadata.getBucket(), metadata.getKey());
                    }
                  });
              }
            });

          AsyncFunction<AccessControlList, S3File> updateObj = new
            AsyncFunction<AccessControlList, S3File>()
            {
              public ListenableFuture<S3File> apply(
                AccessControlList acl) throws IOException
              {
                // It seems there is no way to update an object's metadata
                // without re-uploading/copying the whole object. Here, we
                // copy the object to itself in order to add the new
                // user-metadata.
                CopyOptions options = new CopyOptionsBuilder()
                  .setSourceBucketName(metadata.getBucket())
                  .setSourceKey(metadata.getKey())
                  .setDestinationBucketName(metadata.getBucket())
                  .setDestinationKey(metadata.getKey())
                  .setS3Acl(acl)
                  .setUserMetadata(metadata.getUserMetadata())
                  .createCopyOptions();

                return _client.copy(options);
              }
            };

          return Futures.transform(acl, updateObj);
        }
      };

    return update;
  }
}
