/*
  Copyright 2017, Infor Inc.

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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.codec.digest.DigestUtils;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;


class GCSUploadCommand
  extends Command
{
  private String _encKeyName;
  private String _encryptedSymmetricKeyString;

  private UploadOptions _options;

  private String _objectKey;
  private String _bucketName;

  private OverallProgressListenerFactory _progressListenerFactory;
  private String _pubKeyHash;


  public GCSUploadCommand(UploadOptions options)
    throws IOException
  {
    super(options);
    _options = options;

    this.file = _options.getFile();
    setChunkSize(_options.getChunkSize());
    setFileLength(this.file.length());
    _encKeyName = _options.getEncKey().orElse(null);

    _bucketName = _options.getBucketName();
    _objectKey = _options.getObjectKey();

    if(_encKeyName != null)
    {
      byte[] encKeyBytes = new byte[32];
      new SecureRandom().nextBytes(encKeyBytes);
      this.encKey = new SecretKeySpec(encKeyBytes, "AES");
      try
      {
        Key pubKey = _client.getKeyProvider().getPublicKey(_encKeyName);
        _pubKeyHash = DatatypeConverter.printBase64Binary(
          DigestUtils.sha256(pubKey.getEncoded()));
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, pubKey);
        _encryptedSymmetricKeyString = DatatypeConverter.printBase64Binary(
          cipher.doFinal(encKeyBytes));
      }
      catch(NoSuchKeyException e)
      {
        throw new UsageException("Missing encryption key: " + _encKeyName);
      }
      catch(NoSuchAlgorithmException e)
      {
        throw new RuntimeException(e);
      }
      catch(NoSuchPaddingException e)
      {
        throw new RuntimeException(e);
      }
      catch(InvalidKeyException e)
      {
        throw new RuntimeException(e);
      }
      catch(IllegalBlockSizeException e)
      {
        throw new RuntimeException(e);
      }
      catch(BadPaddingException e)
      {
        throw new RuntimeException(e);
      }
    }

    _progressListenerFactory = _options.getOverallProgressListenerFactory().orElse(null);
  }

  /**
   * Run ties Step 1, Step 2, and Step 3 together. The return result is the ETag of the upload.
   */
  public ListenableFuture<StoreFile> run()
    throws FileNotFoundException
  {
    if(!file.exists())
    {
      throw new FileNotFoundException(file.getPath());
    }

    if(_options.isDryRun())
    {
      System.out.println(
        "<DRYRUN> uploading '" + this.file.getAbsolutePath() + "' to '" + getUri(_bucketName, _objectKey) +
          "'");
      return Futures.immediateFuture(null);
    }
    else
    {
      return scheduleExecution();
    }
  }


  private ListenableFuture<StoreFile> scheduleExecution()
  {
    ListenableFuture<Upload> upload = startUpload();
    upload = Futures.transform(upload, startPartsAsyncFunction());
    ListenableFuture<String> result = Futures.transform(upload, completeAsyncFunction());
    return Futures.transform(result, new Function<String, StoreFile>()
    {
      public StoreFile apply(String etag)
      {
        StoreFile f = new StoreFile();
        f.setLocalFile(file);
        f.setETag(etag);
        f.setBucketName(_bucketName);
        f.setObjectKey(_objectKey);
        return f;
      }
    });
  }

  /**
   * Step 1: Returns a future upload that is internally retried.
   */
  private ListenableFuture<Upload> startUpload()
  {
    return executeWithRetry(_client.getInternalExecutor(), new Callable<ListenableFuture<Upload>>()
    {
      public ListenableFuture<Upload> call()
      {
        return startUploadActual();
      }

      public String toString()
      {
        return "starting upload " + _bucketName + "/" + _objectKey;
      }
    });
  }

  private ListenableFuture<Upload> startUploadActual()
  {
    UploadFactory factory = new GCSUploadFactory(getGCSClient(), _client.getApiExecutor());

    Map<String, String> meta = new HashMap<String, String>();
    meta.put("s3tool-version", String.valueOf(Version.CURRENT));
    if(_encKeyName != null)
    {
      meta.put("s3tool-key-name", _encKeyName);
      meta.put("s3tool-symmetric-key", _encryptedSymmetricKeyString);
      meta.put("s3tool-pubkey-hash", _pubKeyHash.substring(0, 8));
    }
    // single-part => chunk size == file size
    meta.put("s3tool-chunk-size", Long.toString(fileLength));
    meta.put("s3tool-file-length", Long.toString(fileLength));

    return factory.startUpload(_bucketName, _objectKey, meta, _options);
  }

  /**
   * Step 2: Upload parts
   */
  private AsyncFunction<Upload, Upload> startPartsAsyncFunction()
  {
    return new AsyncFunction<Upload, Upload>()
    {
      public ListenableFuture<Upload> apply(Upload upload)
      {
        return startParts(upload);
      }
    };
  }

  private ListenableFuture<Upload> startParts(final Upload upload)
  {
    OverallProgressListener opl = null;
    if(_progressListenerFactory != null)
    {
      opl = _progressListenerFactory.create(
        new ProgressOptionsBuilder().setObjectUri(getUri(upload.getBucketName(), upload.getObjectKey()))
          .setOperation("upload")
          .setFileSizeInBytes(fileLength)
          .createProgressOptions());
    }

    ListenableFuture<Void> part = startPartUploadThread(upload, opl);

    // we do not care about the voids, so we just return the upload
    // object.
    return Futures.transform(part, Functions.constant(upload));
  }

  private ListenableFuture<Void> startPartUploadThread(
    final Upload upload, final OverallProgressListener opl)
  {
    ListenableFuture<ListenableFuture<Void>> result = _client.getInternalExecutor()
      .submit(new Callable<ListenableFuture<Void>>()
      {
        public ListenableFuture<Void> call()
          throws Exception
        {
          return GCSUploadCommand.this.startPartUpload(upload, opl);
        }
      });

    return Futures.dereference(result);
  }

  /**
   * Execute startPartUpload with retry
   */
  private ListenableFuture<Void> startPartUpload(
    final Upload upload, final OverallProgressListener opl)
  {
    final int partNumber = 0;

    return executeWithRetry(_client.getInternalExecutor(), new Callable<ListenableFuture<Void>>()
    {
      public ListenableFuture<Void> call()
        throws Exception
      {
        return startPartUploadActual(upload, opl);
      }

      public String toString()
      {
        return "uploading part " + (partNumber + 1);
      }
    });
  }

  private ListenableFuture<Void> startPartUploadActual(
    final Upload upload, final OverallProgressListener opl)
    throws Exception
  {
    final int partNumber = 0;
    final Cipher cipher;

    long partSize;
    if(_encKeyName != null)
    {
      cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

      long preCryptSize = fileLength;
      long blockSize = cipher.getBlockSize();
      partSize = blockSize * (preCryptSize / blockSize + 2);
    }
    else
    {
      cipher = null;
      partSize = fileLength;
    }

    Callable<InputStream> inputStreamCallable = new Callable<InputStream>()
    {
      public InputStream call()
        throws Exception
      {
        FileInputStream fs = new FileInputStream(file);

        BufferedInputStream bs = new BufferedInputStream(fs);
        InputStream in;
        if(cipher != null)
        {
          in = new CipherWithInlineIVInputStream(bs, cipher, Cipher.ENCRYPT_MODE, encKey);
        }
        else
        {
          in = bs;
        }

        return in;
      }
    };

    return upload.uploadPart(partNumber, partSize, inputStreamCallable, opl);
  }

  /**
   * Step 3: Complete parts
   */
  private AsyncFunction<Upload, String> completeAsyncFunction()
  {
    return new AsyncFunction<Upload, String>()
    {
      public ListenableFuture<String> apply(Upload upload)
      {
        return complete(upload, 0);
      }
    };
  }

  /**
   * Execute completeActual with retry
   */
  private ListenableFuture<String> complete(final Upload upload, final int retryCount)
  {
    return executeWithRetry(_client.getInternalExecutor(), new Callable<ListenableFuture<String>>()
    {
      public ListenableFuture<String> call()
      {
        return completeActual(upload, retryCount);
      }

      public String toString()
      {
        return "completing upload";
      }
    });
  }

  private ListenableFuture<String> completeActual(final Upload upload, final int retryCount)
  {
    return upload.completeUpload();
  }
}
