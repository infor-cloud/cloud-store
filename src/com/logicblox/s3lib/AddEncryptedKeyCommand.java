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
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

// TODO(geokollias): Maybe rename to AddEncryptionKeyCommand ?
public class AddEncryptedKeyCommand extends Command
{
  private final Logger _logger;
  private CloudStoreClient _client;
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private String _encKeyName;
  private KeyProvider _encKeyProvider;

  public AddEncryptedKeyCommand(
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

    _logger = LoggerFactory.getLogger(AddEncryptedKeyCommand.class);
  }

  public ListenableFuture<S3File> run(final String bucket,final String key,
                                      final String version)
  {
    // TODO(geokollias): Handle versions?
    ListenableFuture<S3ObjectMetadata> objMeta = getMetadata(bucket, key);
    objMeta = Futures.transform(objMeta, addNewEncryptionKeyFn());
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
          return "Starting addition of new encryption key to " +
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
                                     "object has multiple encrypted keys");
          }
          return Futures.immediateFuture(metadata);
        }
      };

    return Futures.transform(metadataFactory, checkMetadata);
  }

  /**
   * Step 2: Add new encrypted key
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
            return addNewEncryptedKey(metadata);
          }
        };

        return _executor.submit(addNewEncyptionKeyCall);
      }
    };
  }

  private S3ObjectMetadata addNewEncryptedKey(S3ObjectMetadata metadata)
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
    Map<String, String> userMetadata = new LinkedHashMap(
      metadata.getUserMetadata());
    _logger.debug("userMetadata = {}", userMetadata);
    String keyNamesStr = userMetadata.get("s3tool-key-name");
    List<String> keyNames = new ArrayList<>(Arrays.asList(
      keyNamesStr.split(",")));
    String pubKeyHashesStr = userMetadata.get("s3tool-pubkey-hash");
    List<String> pubKeyHashes = new ArrayList<>(Arrays.asList(
      pubKeyHashesStr.split(",")));
    if (keyNames.contains(_encKeyName))
    {
      throw new UsageException(errPrefix + _encKeyName + " already exists.");
    }
    int maxAllowedKeys = 4;
    if (keyNames.size() >= maxAllowedKeys)
    {
      throw new UsageException(errPrefix + "No more than " + maxAllowedKeys +
        " keys are allowed.");
    }
    PrivateKey privKey = null;
    int privKeyIndex = -1;
    boolean privKeyFound = false;
    for (String kn : keyNames)
    {
      privKeyIndex++;
      try
      {
        privKey = _encKeyProvider.getPrivateKey(kn);
      }
      catch (NoSuchKeyException e)
      {
        // We might find an eligible key later.
        continue;
      }

      try
      {
        PublicKey pubKey = Command.getPublicKey(privKey);
        String pubKeyHashLocal = DatatypeConverter.printBase64Binary(
          DigestUtils.sha256(pubKey.getEncoded())).substring(0, 8);

        if (pubKeyHashLocal.equals(pubKeyHashes.get(privKeyIndex)))
        {
          // Successfully-read, validated key.
          privKeyFound = true;
          break;
        }
      }
      catch (NoSuchKeyException e)
      {
        throw new UsageException(errPrefix + "Cannot generate the public key " +
                                 "out of the private one for '" + kn);
      }
    }

    if (privKey == null || !privKeyFound)
    {
      // No private key found
      throw new UsageException(errPrefix + "No eligible private key found");
    }

    Cipher cipher;
    byte[] encKeyBytes;
    String symKeysStr = userMetadata.get("s3tool-symmetric-key");
    List<String> symKeys = new ArrayList<>(Arrays.asList(
      symKeysStr.split(",")));
    try
    {
      cipher = Cipher.getInstance("RSA");
      cipher.init(Cipher.DECRYPT_MODE, privKey);
      // TODO(geokollias): Is it possible to have "privKey != null" &&
      // privKeyIndex out of symKeys bounds? Add assertion.
      encKeyBytes = cipher.doFinal(DatatypeConverter.parseBase64Binary(
        symKeys.get(privKeyIndex)));
    }
    catch (
      InvalidKeyException |
      BadPaddingException |
      NoSuchPaddingException |
      NoSuchAlgorithmException |
      IllegalBlockSizeException e)
    {
      throw new RuntimeException(e);
    }

    encKey = new SecretKeySpec(encKeyBytes, "AES");
    String encSymKeyString;
    String pubKeyHash;
    try
    {
      PublicKey pubKey = _encKeyProvider.getPublicKey(_encKeyName);
      pubKeyHash = DatatypeConverter.printBase64Binary(
        DigestUtils.sha256(pubKey.getEncoded())).substring(0, 8);

      cipher = Cipher.getInstance("RSA");
      cipher.init(Cipher.ENCRYPT_MODE, pubKey);
      encSymKeyString = DatatypeConverter.printBase64Binary(
        cipher.doFinal(encKeyBytes));
    }
    catch (NoSuchKeyException e)
    {
      throw new UsageException(errPrefix + "Missing encryption key " +
                                 _encKeyName);
    }
    catch (
      InvalidKeyException |
      BadPaddingException |
      NoSuchPaddingException |
      NoSuchAlgorithmException |
      IllegalBlockSizeException e)
    {
      throw new RuntimeException(e);
    }

    if ((pubKeyHash == null) || (encSymKeyString == null))
    {
      throw new RuntimeException(errPrefix + " Failed to add new key");
    }

    // Update user-metadata
    ObjectMetadata allMeta = metadata.getAllMetadata();

    keyNames.add(_encKeyName);
    allMeta.addUserMetadata("s3tool-key-name",
      Joiner.on(",").join(keyNames));

    symKeys.add(encSymKeyString);
    allMeta.addUserMetadata("s3tool-symmetric-key",
      Joiner.on(",").join(symKeys));

    pubKeyHashes.add(pubKeyHash);
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
                      AccessControlList acl = getAmazonS3Client().getObjectAcl(
                        metadata.getBucket(), metadata.getKey());

                      return acl;
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
