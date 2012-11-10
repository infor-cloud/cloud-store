package com.logicblox.s3lib;

import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;
import java.io.RandomAccessFile;
import java.io.ObjectInputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import java.security.SecureRandom;
import java.security.Key;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidAlgorithmParameterException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import com.amazonaws.services.s3.AmazonS3Client;

import com.google.common.base.Functions;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class UploadCommand extends Command
{
  private String encKeyName;
  private String encryptedSymmetricKeyString;

  private ListeningExecutorService _uploadExecutor;
  private ListeningExecutorService _executor;
  
  public UploadCommand(
    ListeningExecutorService uploadExecutor,
    ListeningExecutorService internalExecutor,
    File file,
    long chunkSize,
    String encKeyName,
    KeyProvider encKeyProvider)
  throws IOException
  {
    if(uploadExecutor == null)
      throw new IllegalArgumentException("non-null upload executor is required");
    if(internalExecutor == null)
      throw new IllegalArgumentException("non-null internal executor is required");

    _uploadExecutor = uploadExecutor;
    _executor = internalExecutor;

    this.file = file;
    setChunkSize(chunkSize);
    this.fileLength = file.length();
    this.encKeyName = encKeyName;

    if (this.encKeyName != null) {
      byte[] encKeyBytes = new byte[32];
      new SecureRandom().nextBytes(encKeyBytes);
      this.encKey = new SecretKeySpec(encKeyBytes, "AES");
      try
      {
        Key pubKey = encKeyProvider.getPublicKey(this.encKeyName);
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, pubKey);
        this.encryptedSymmetricKeyString = DatatypeConverter.printBase64Binary(cipher.doFinal(encKeyBytes));
      } catch (NoSuchKeyException e) {
        throw new UsageException("Missing encryption key: " + this.encKeyName);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      } catch (NoSuchPaddingException e) {
        throw new RuntimeException(e);
      } catch (InvalidKeyException e) {
        throw new RuntimeException(e);
      } catch (IllegalBlockSizeException e) {
        throw new RuntimeException(e);
      } catch (BadPaddingException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  /**
   * Run ties Step 1, Step 2, and Step 3 together. The return result is the ETag of the upload.
   */
  public ListenableFuture<String> run(final String bucket, final String key) throws FileNotFoundException
  {
    if (!file.exists())
      throw new FileNotFoundException(file.getPath());

    if (this.fileLength == 0) {
      throw new UsageException("File does not exist or is a special file");
    }

    System.out.println("Initiating upload");
    ListenableFuture<Upload> upload = startUpload(bucket, key, 0);
    upload = Futures.transform(upload, startPartsAsyncFunction());
    ListenableFuture<String> result = Futures.transform(upload, completeAsyncFunction());
    return result;
  }

  /**
   * Step 1: Returns a future upload that is internally retried.
   */
  public ListenableFuture<Upload> startUpload(final String bucket, final String key, final int retryCount)
  throws FileNotFoundException
  {
    UploadFactory factory = new MultipartAmazonUploadFactory(new AmazonS3Client(), _uploadExecutor);
    
    Map<String,String> meta = new HashMap<String,String>();
    meta.put("s3tool-version", String.valueOf(Version.CURRENT));
    if (this.encKeyName != null) {
      meta.put("s3tool-key-name", encKeyName);
      meta.put("s3tool-symmetric-key", encryptedSymmetricKeyString);
    }
    meta.put("s3tool-chunk-size", Long.toString(chunkSize));
    meta.put("s3tool-file-length", Long.toString(fileLength));

    // TODO should we schedule the retry after a pause?              
    ListenableFuture<Upload> startUploadFuture = factory.startUpload(bucket, key, meta);

    FutureFallback<Upload> startAgain = new FutureFallback<Upload>()
      {
        public ListenableFuture<Upload> create(Throwable thrown) throws Exception
        {
          System.err.println("Error starting upload: " + thrown.getMessage());
            
            rethrowOnMaxRetry(thrown, retryCount);
            return UploadCommand.this.startUpload(bucket, key, retryCount + 1);
        }
      };
    
    return withFallback(startUploadFuture, startAgain);
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

  public ListenableFuture<Upload> startParts(final Upload upload)
  {
    List<ListenableFuture<Void>> parts = new ArrayList<ListenableFuture<Void>>();
    
    for (long position = 0; position < fileLength; position += chunkSize)
    {
      parts.add(startPartUploadThread(upload, position));
    }

    // we do not care about the voids, so we just return the upload
    // object.
    return Futures.transform(
      Futures.allAsList(parts),
      Functions.constant(upload));
  }
  
  public ListenableFuture<Void> startPartUploadThread(final Upload upload, final long position)
  {
    ListenableFuture<ListenableFuture<Void>> result =
      _executor.submit(new Callable<ListenableFuture<Void>>()
        {
          public ListenableFuture<Void> call() throws Exception
          {
            return UploadCommand.this.startPartUpload(upload, position, 0);
          }
        });

    return Futures.dereference(result);
  }
  
  public ListenableFuture<Void> startPartUpload(final Upload upload, final long position, final int retryCount)
  throws FileNotFoundException
  {
    final int partNumber = (int) (position / chunkSize);
    final FileInputStream fs = new FileInputStream(file);

    try {
      long skipped = fs.skip(position);
      while (skipped < position) {
        skipped += fs.skip(position - skipped);
      }
    } catch (IOException e) {
      System.err.println("Error uploading part " + partNumber + ": " + e.getMessage());
      try {
        fs.close();
      } catch (IOException ignored) {
      }
      return startPartUpload(upload, position, retryCount + 1);
    }

    BufferedInputStream bs = new BufferedInputStream(fs);
    InputStream in;
    long partSize;
    if (this.encKeyName != null) {
      Cipher cipher = null;
      try {
        cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      } catch (NoSuchPaddingException e) {
        throw new RuntimeException(e);
      }

      try {
        in = new CipherWithInlineIVInputStream(bs, cipher, Cipher.ENCRYPT_MODE, encKey);
      } catch (IOException e) {
        System.err.println("Error uploading part " + partNumber + ": " + e.getMessage());
        try {
          fs.close();
        } catch (IOException ignored) {
        }
        return startPartUpload(upload, position, retryCount + 1);
      } catch (InvalidKeyException e) {
        throw new RuntimeException(e);
      } catch (InvalidAlgorithmParameterException e) {
        System.err.println("Error uploading part " + partNumber + ": " + e.getMessage());
        try {
          fs.close();
        } catch (Exception ignored) {
        }
        return startPartUpload(upload, position, retryCount + 1);
      }

      long preCryptSize = Math.min(fileLength - position, chunkSize);
      long blockSize = cipher.getBlockSize();
      partSize = blockSize * (preCryptSize/blockSize + 2);
    } else {
      in = bs;
      partSize = Math.min(fileLength - position, chunkSize);
    }

    // TODO should we schedule the retry after a pause based on retryCount?
    System.out.println("Uploading part " + partNumber);
    ListenableFuture<Void> uploadPartFuture = upload.uploadPart(partNumber, in, partSize);

    FutureFallback<Void> uploadPartAgain = new FutureFallback<Void>()
      {
        public ListenableFuture<Void> create(Throwable thrown) throws Exception
        {
          System.err.println("Error uploading part " + partNumber + ": " + thrown.getMessage());
            
            try {
              fs.close();
            } catch (Exception e) {
            }
            
            rethrowOnMaxRetry(thrown, retryCount);
            return UploadCommand.this.startPartUpload(upload, position, retryCount + 1);
        }
      };

    Futures.addCallback(uploadPartFuture, new FutureCallback<Void>()
      {
        public void onFailure(Throwable t) {}
        public void onSuccess(Void ignored) {
          try {
            fs.close();
          } catch (Exception e) {
          }
        }
      });
    
    return withFallback(uploadPartFuture, uploadPartAgain);
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

  public ListenableFuture<String> complete(final Upload upload, final int retryCount)
  {
    System.out.println("Finished all parts, now completing upload");
    // TODO should we schedule the retry after a pause?
    ListenableFuture<String> completeUploadFuture = upload.completeUpload();

    FutureFallback<String> completeAgain = new FutureFallback<String>()
    {
      public ListenableFuture<String> create(Throwable thrown) throws Exception
      {
        System.err.println("Error completing upload: " + thrown.getMessage());
        rethrowOnMaxRetry(thrown, retryCount);
        return UploadCommand.this.complete(upload, retryCount + 1);
      }
    };

    return withFallback(completeUploadFuture, completeAgain);
  }
}
