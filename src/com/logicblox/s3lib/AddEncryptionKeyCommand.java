package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.codec.digest.DigestUtils;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class AddEncryptionKeyCommand extends Command
{
  private EncryptionKeyOptions _options;
  private String _encKeyName;
  private KeyProvider _encKeyProvider;

  public AddEncryptionKeyCommand(EncryptionKeyOptions options)
  throws IOException
  {
    super(options);
    _options = options;
    _encKeyProvider = _client.getKeyProvider();
    _encKeyName = _options.getEncryptionKey();
  }

  public ListenableFuture<S3File> run()
  {
    // TODO(geokollias): Handle versions?
    ListenableFuture<S3ObjectMetadata> objMeta = getMetadata();
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
              "adding new encryption key to " + getUri(_options.getBucketName(),
            _options.getObjectKey())+ ".", t));
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
          return "Starting addition of new encryption key to " +
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
    if(null == keyNamesStr)
      keyNamesStr = "";
    List<String> keyNames = new ArrayList<>(Arrays.asList(keyNamesStr.split(",")));
    String pubKeyHashesStr = userMetadata.get("s3tool-pubkey-hash");
    if(null == pubKeyHashesStr)
      pubKeyHashesStr = "";
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
          if(null == getGCSClient())
          {
            // It seems for AWS there is no way to update an object's metadata
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
              new Callable<ListenableFuture<S3File>>()
              {
                public ListenableFuture<S3File> call()
                {
                  return _client.getApiExecutor().submit(new Callable<S3File>()
                  {
                    public S3File call()
                    throws IOException
                    {
                      GCSClient.patchMetaData(getGCSClient(), metadata.getBucket(),
                        metadata.getKey(), metadata.getUserMetadata());
                      return new S3File(metadata.getBucket(), metadata.getKey());
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
