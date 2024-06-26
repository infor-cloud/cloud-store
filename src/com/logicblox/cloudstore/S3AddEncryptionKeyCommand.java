/*
  Copyright 2018, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.cloudstore;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.AsyncFunction;
//import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.codec.digest.DigestUtils;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

class S3AddEncryptionKeyCommand
  extends Command
{
  private static final Base64.Decoder base64Decoder = Base64.getMimeDecoder();
  private static final Base64.Encoder base64Encoder = Base64.getEncoder();

  private EncryptionKeyOptions _options;
  private String _encKeyName;
  private KeyProvider _encKeyProvider;

  public S3AddEncryptionKeyCommand(EncryptionKeyOptions options)
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
//    objMeta = Futures.transform(objMeta, addNewEncryptionKeyFn());
    objMeta = Futures.transformAsync(
      objMeta, addNewEncryptionKeyFn(), MoreExecutors.directExecutor());
//    ListenableFuture<StoreFile> res = Futures.transform(objMeta, updateObjectMetadataFn());
    ListenableFuture<StoreFile> res = Futures.transformAsync(
      objMeta, updateObjectMetadataFn(), MoreExecutors.directExecutor());

//    return Futures.withFallback(res, new FutureFallback<StoreFile>()
    return Futures.catchingAsync(
      res, 
      Throwable.class, new AsyncFunction<Throwable, StoreFile>()
      {
//      public ListenableFuture<StoreFile> create(Throwable t)
        public ListenableFuture<StoreFile> apply(Throwable t)
        {
          if(t instanceof UsageException)
          {
            return Futures.immediateFailedFuture(t);
          }
          return Futures.immediateFailedFuture(new Exception(
            "Error " + "adding new encryption key to " +
              getUri(_options.getBucketName(), _options.getObjectKey()) + ".", t));
        }
      },
      MoreExecutors.directExecutor());
  }

  /**
   * Step 1: Fetch metadata.
   */
  private ListenableFuture<S3ObjectMetadata> getMetadata()
  {
    return executeWithRetry(_client.getInternalExecutor(),
      new Callable<ListenableFuture<S3ObjectMetadata>>()
      {
        public ListenableFuture<S3ObjectMetadata> call()
        {
          return getMetadataAsync();
        }

        public String toString()
        {
          return "Starting addition of new encryption key to " +
            getUri(_options.getBucketName(), _options.getObjectKey());
        }
      });
  }

  private ListenableFuture<S3ObjectMetadata> getMetadataAsync()
  {
    if(null == getGCSClient())
      return getMetadataAsyncAws();
    else
      return getMetadataAsyncGcs();
  }

  private ListenableFuture<S3ObjectMetadata> getMetadataAsyncGcs()
  {
    final String bucket = _options.getBucketName();
    final String objectKey = _options.getObjectKey();
    ExistsOptions opts = new ExistsOptions(_options.getCloudStoreClient(), bucket, objectKey);
    ListenableFuture<Metadata> mdFuture = _client.exists(opts);

    AsyncFunction<Metadata, S3ObjectMetadata> convert = new AsyncFunction<Metadata, S3ObjectMetadata>()
    {
      public ListenableFuture<S3ObjectMetadata> apply(Metadata metadata)
      {
        S3ObjectMetadata s3Md = new S3ObjectMetadata(
           getS3Client(), objectKey, bucket, null, metadata.getObjectMetadata(),
           _client.getApiExecutor());

        // check that key info is in the metadata
        Map<String, String> userMetadata = metadata.getUserMetadata();
        String obj = getUri(bucket, objectKey);
        if(!userMetadata.containsKey("s3tool-key-name"))
          throw new UsageException("Object doesn't seem to be encrypted");
        if(!userMetadata.containsKey("s3tool-pubkey-hash"))
          throw new UsageException(
            "Public key hashes are required when " + "object has multiple encryption keys");

        return Futures.immediateFuture(s3Md);
      }
    };

    return Futures.transformAsync(mdFuture, convert, MoreExecutors.directExecutor());
  }

  private ListenableFuture<S3ObjectMetadata> getMetadataAsyncAws()
  {
    // FIXME - Not sure why this isn't using the exists command like I'm doing above
    // for GCS.  Maybe this should be changed to match?
 
    S3ObjectMetadataFactory f = new S3ObjectMetadataFactory(getS3Client(),
      _client.getApiExecutor());
    ListenableFuture<S3ObjectMetadata> metadataFactory = f.create(_options.getBucketName(),
      _options.getObjectKey(), null);

    AsyncFunction<S3ObjectMetadata, S3ObjectMetadata> checkMetadata
      = new AsyncFunction<S3ObjectMetadata, S3ObjectMetadata>()
    {
      public ListenableFuture<S3ObjectMetadata> apply(S3ObjectMetadata metadata)
      {
        Map<String, String> userMetadata = metadata.getUserMetadata();
        String obj = getUri(metadata.getBucketName(), metadata.getObjectKey());
        if(!userMetadata.containsKey("s3tool-key-name"))
        {
          throw new UsageException("Object doesn't seem to be encrypted");
        }
        if(!userMetadata.containsKey("s3tool-pubkey-hash"))
        {
          throw new UsageException(
            "Public key hashes are required when " + "object has multiple encryption keys");
        }
        return Futures.immediateFuture(metadata);
      }
    };

//    return Futures.transform(metadataFactory, checkMetadata);
    return Futures.transformAsync(metadataFactory, checkMetadata, MoreExecutors.directExecutor());
  }


  /**
   * Step 2: Add new encryption key
   */
  private AsyncFunction<S3ObjectMetadata, S3ObjectMetadata> addNewEncryptionKeyFn()
  {
    return new AsyncFunction<S3ObjectMetadata, S3ObjectMetadata>()
    {
      public ListenableFuture<S3ObjectMetadata> apply(final S3ObjectMetadata metadata)
      {
        Callable addNewEncyptionKeyCall = new Callable<S3ObjectMetadata>()
        {
          public S3ObjectMetadata call()
          {
            return addNewEncryptionKey(metadata);
          }
        };

        return _client.getInternalExecutor().submit(addNewEncyptionKeyCall);
      }
    };
  }

  private S3ObjectMetadata addNewEncryptionKey(S3ObjectMetadata metadata)
  {
    String errPrefix = getUri(metadata.getBucketName(), metadata.getObjectKey()) + ": ";
    if(_encKeyProvider == null)
    {
      throw new UsageException(errPrefix + "No encryption key provider is " + "specified");
    }
    if(_encKeyName == null)
    {
      throw new UsageException(errPrefix + "No encryption key name is " + "specified");
    }
    Map<String, String> userMetadata = metadata.getUserMetadata();
    String keyNamesStr = userMetadata.get("s3tool-key-name");
    if(null == keyNamesStr)
    {
      keyNamesStr = "";
    }
    List<String> keyNames = new ArrayList<>(Arrays.asList(keyNamesStr.split(",")));
    String pubKeyHashesStr = userMetadata.get("s3tool-pubkey-hash");
    if(null == pubKeyHashesStr)
    {
      pubKeyHashesStr = "";
    }
    List<String> pubKeyHashes = new ArrayList<>(Arrays.asList(pubKeyHashesStr.split(",")));
    if(keyNames.contains(_encKeyName))
    {
      throw new UsageException(errPrefix + _encKeyName + " already exists.");
    }
    int maxAllowedKeys = 4;
    if(keyNames.size() >= maxAllowedKeys)
    {
      throw new UsageException(errPrefix + "No more than " + maxAllowedKeys + " keys are allowed.");
    }
    PrivateKey privKey = null;
    int privKeyIndex = -1;
    boolean privKeyFound = false;
    for(String kn : keyNames)
    {
      privKeyIndex++;
      try
      {
        privKey = _encKeyProvider.getPrivateKey(kn);
      }
      catch(NoSuchKeyException e)
      {
        // We might find an eligible key later.
        continue;
      }

      try
      {
        PublicKey pubKey = Command.getPublicKey(privKey);
        String pubKeyHashLocal = base64Encoder.encodeToString(
          DigestUtils.sha256(pubKey.getEncoded())).substring(0, 8);

        if(pubKeyHashLocal.equals(pubKeyHashes.get(privKeyIndex)))
        {
          // Successfully-read, validated key.
          privKeyFound = true;
          break;
        }
      }
      catch(NoSuchKeyException e)
      {
        throw new UsageException(
          errPrefix + "Cannot generate the public key " + "out of the private one for '" + kn);
      }
    }

    if(privKey == null || !privKeyFound)
    {
      // No private key found
      throw new UsageException(errPrefix + "No eligible private key found");
    }

    Cipher cipher;
    byte[] encKeyBytes;
    String symKeysStr = userMetadata.get("s3tool-symmetric-key");
    List<String> symKeys = new ArrayList<>(Arrays.asList(symKeysStr.split(",")));
    try
    {
      cipher = Cipher.getInstance("RSA");
      cipher.init(Cipher.DECRYPT_MODE, privKey);
      // TODO(geokollias): Is it possible to have "privKey != null" &&
      // privKeyIndex out of symKeys bounds? Add assertion.
      encKeyBytes = cipher.doFinal(base64Decoder.decode(symKeys.get(privKeyIndex)));
    }
    catch(InvalidKeyException | BadPaddingException | NoSuchPaddingException |
      NoSuchAlgorithmException | IllegalBlockSizeException e)
    {
      throw new RuntimeException(e);
    }

    encKey = new SecretKeySpec(encKeyBytes, "AES");
    String encSymKeyString;
    String pubKeyHash;
    try
    {
      PublicKey pubKey = _encKeyProvider.getPublicKey(_encKeyName);
      pubKeyHash = base64Encoder.encodeToString(DigestUtils.sha256(pubKey.getEncoded()))
        .substring(0, 8);

      cipher = Cipher.getInstance("RSA");
      cipher.init(Cipher.ENCRYPT_MODE, pubKey);
      encSymKeyString = base64Encoder.encodeToString(cipher.doFinal(encKeyBytes));
    }
    catch(NoSuchKeyException e)
    {
      throw new UsageException(errPrefix + "Missing encryption key " + _encKeyName);
    }
    catch(InvalidKeyException | BadPaddingException | NoSuchPaddingException |
      NoSuchAlgorithmException | IllegalBlockSizeException e)
    {
      throw new RuntimeException(e);
    }

    if((pubKeyHash == null) || (encSymKeyString == null))
    {
      throw new RuntimeException(errPrefix + " Failed to add new key");
    }

    // Update user-metadata
    ObjectMetadata allMeta = metadata.getAllMetadata();

    keyNames.add(_encKeyName);
    allMeta.addUserMetadata("s3tool-key-name", Joiner.on(",").join(keyNames));

    symKeys.add(encSymKeyString);
    allMeta.addUserMetadata("s3tool-symmetric-key", Joiner.on(",").join(symKeys));

    pubKeyHashes.add(pubKeyHash);
    allMeta.addUserMetadata("s3tool-pubkey-hash", Joiner.on(",").join(pubKeyHashes));

    return metadata;
  }

  /**
   * Step 3: Update object's metadata
   */
  private AsyncFunction<S3ObjectMetadata, StoreFile> updateObjectMetadataFn()
  {
    AsyncFunction<S3ObjectMetadata, StoreFile> update
      = new AsyncFunction<S3ObjectMetadata, StoreFile>()
    {
      public ListenableFuture<StoreFile> apply(final S3ObjectMetadata metadata)
        throws IOException
      {
        if(null == getGCSClient())
        {
          // It seems for AWS there is no way to update an object's metadata
          // without re-uploading/copying the whole object. Here, we
          // copy the object to itself in order to add the new
          // user-metadata.
          CopyOptions options = _client.getOptionsBuilderFactory()
            .newCopyOptionsBuilder()
            .setSourceBucketName(metadata.getBucketName())
            .setSourceObjectKey(metadata.getObjectKey())
            .setDestinationBucketName(metadata.getBucketName())
            .setDestinationObjectKey(metadata.getObjectKey())
            .setUserMetadata(metadata.getUserMetadata())
            .createOptions();

          return _client.copy(options);
        }
        else
        {
          return executeWithRetry(_client.getInternalExecutor(),
            new Callable<ListenableFuture<StoreFile>>()
            {
              public ListenableFuture<StoreFile> call()
              {
                return _client.getApiExecutor().submit(new Callable<StoreFile>()
                {
                  public StoreFile call()
                    throws IOException
                  {
                    GCSClient.patchMetaData(getGCSClient(), metadata.getBucketName(), metadata.getObjectKey(),
                      metadata.getUserMetadata());
                    return new StoreFile(metadata.getBucketName(), metadata.getObjectKey());
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
