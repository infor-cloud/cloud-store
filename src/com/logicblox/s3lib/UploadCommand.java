package com.logicblox.s3lib;

import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Callable;

import java.security.SecureRandom;
import java.security.Key;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.commons.codec.digest.DigestUtils;

public class UploadCommand extends Command
{
  private String encKeyName;
  private String encryptedSymmetricKeyString;
  private String acl;
  private Optional<OverallProgressListenerFactory> progressListenerFactory;
  private String pubKeyHash;

  private ListeningExecutorService _uploadExecutor;
  private ListeningScheduledExecutorService _executor;
  private UploadOptions _options;


  public UploadCommand(UploadOptions options)
  throws IOException
  {
    _options = options;
    _uploadExecutor = _options.getCloudStoreClient().getApiExecutor();
    _executor = _options.getCloudStoreClient().getInternalExecutor();

    this.file = _options.getFile();
    setChunkSize(_options.getChunkSize());
    setFileLength(this.file.length());
    this.encKeyName = _options.getEncKey().orNull();

    if (this.encKeyName != null) {
      byte[] encKeyBytes = new byte[32];
      new SecureRandom().nextBytes(encKeyBytes);
      this.encKey = new SecretKeySpec(encKeyBytes, "AES");
      try
      {
        if (_options.getCloudStoreClient().getKeyProvider() == null)
          throw new UsageException("No encryption key provider is specified");
        Key pubKey = _options.getCloudStoreClient().getKeyProvider().getPublicKey(this.encKeyName);

        this.pubKeyHash = DatatypeConverter.printBase64Binary(
          DigestUtils.sha256(pubKey.getEncoded()));

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

    this.acl = _options.getAcl().orNull();
    this.progressListenerFactory = _options.getOverallProgressListenerFactory();
  }

  /**
   * Run ties Step 1, Step 2, and Step 3 together. The return result is the ETag of the upload.
   */
  public ListenableFuture<S3File> run()
    throws FileNotFoundException
  {
    if (!file.exists())
      throw new FileNotFoundException(file.getPath());

    if(_options.isDryRun())
    {
      System.out.println("<DRYRUN> uploading '" + this.file.getAbsolutePath()
        + "' to '" + getUri(_options.getBucket(), _options.getObjectKey()) + "'");
      return Futures.immediateFuture(null);
    }
    else
    {
      return scheduleExecution();
    }
  }
  

  private ListenableFuture<S3File> scheduleExecution()
  {
    final ListenableFuture<Upload> started = startUpload();
    ListenableFuture<Upload> uploaded = Futures.transform(started, startPartsAsyncFunction());
    ListenableFuture<String> completed = Futures.transform(uploaded, completeAsyncFunction());
    ListenableFuture<S3File> res = Futures.transform(completed,
      new Function<String, S3File>()
      {
        public S3File apply(String etag)
        {
          S3File f = new S3File();
          f.setLocalFile(file);
          f.setETag(etag);
          f.setBucketName(_options.getBucket());
          f.setKey(_options.getObjectKey());
          return f;
        }
      });

    return Futures.withFallback(
      res,
      new FutureFallback<S3File>()
      {
        public ListenableFuture<S3File> create(final Throwable t)
        {
          ListenableFuture<Void> aborted = Futures.transform(started, abortAsyncFunction());
          ListenableFuture<S3File> res0 = Futures.transform(aborted,
            new AsyncFunction<Void, S3File>()
            {
              public ListenableFuture<S3File> apply(Void v)
              {
                return Futures.immediateFailedFuture(t);
              }
            });

          return res0;
        }
      },
      _executor);
  }

  /**
   * Step 1: Returns a future upload that is internally retried.
   */
  private ListenableFuture<Upload> startUpload()
  {
    return executeWithRetry(_executor,
      new Callable<ListenableFuture<Upload>>()
      {
        public ListenableFuture<Upload> call()
        {
          return startUploadActual();
        }

        public String toString()
        {
          return "starting upload " + _options.getBucket() + "/" + _options.getObjectKey();
        }
      });
  }

  private ListenableFuture<Upload> startUploadActual()
  {
    UploadFactory factory = new MultipartAmazonUploadFactory
        (getAmazonS3Client(), _uploadExecutor);

    Map<String,String> meta = new HashMap<String,String>();
    meta.put("s3tool-version", String.valueOf(Version.CURRENT));
    if (this.encKeyName != null) {
      meta.put("s3tool-key-name", encKeyName);
      meta.put("s3tool-symmetric-key", encryptedSymmetricKeyString);
      meta.put("s3tool-pubkey-hash", pubKeyHash.substring(0,8));
    }
    meta.put("s3tool-chunk-size", Long.toString(chunkSize));
    meta.put("s3tool-file-length", Long.toString(fileLength));

    return factory.startUpload(_options.getBucket(), _options.getObjectKey(), meta, acl, _options);
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
    if (progressListenerFactory.isPresent()) {
      opl = progressListenerFactory.get().create(
          new ProgressOptionsBuilder()
              .setObjectUri(getUri(upload.getBucket(), upload.getKey()))
              .setOperation("upload")
              .setFileSizeInBytes(fileLength)
              .createProgressOptions());
    }

    List<ListenableFuture<Void>> parts = new ArrayList<ListenableFuture<Void>>();
    for (long position = 0;
         position < fileLength || (position == 0 && fileLength == 0);
         position += chunkSize)
    {
      parts.add(startPartUploadThread(upload, position, opl));
    }

    // we do not care about the voids, so we just return the upload
    // object.
    return Futures.transform(
      Futures.allAsList(parts),
      Functions.constant(upload));
  }

  private ListenableFuture<Void> startPartUploadThread(final Upload upload,
                                                       final long position,
                                                       final OverallProgressListener opl)
  {
    ListenableFuture<ListenableFuture<Void>> result =
      _executor.submit(new Callable<ListenableFuture<Void>>()
        {
          public ListenableFuture<Void> call() throws Exception
          {
            return UploadCommand.this.startPartUpload(upload, position, opl);
          }
        });

    return Futures.dereference(result);
  }

  /**
   * Execute startPartUpload with retry
   */
  private ListenableFuture<Void> startPartUpload(final Upload upload,
                                                 final long position,
                                                 final OverallProgressListener opl)
  {
    final int partNumber = (int) (position / chunkSize);

    return executeWithRetry(_executor,
      new Callable<ListenableFuture<Void>>()
      {
        public ListenableFuture<Void> call() throws Exception
        {
          return startPartUploadActual(upload, position, opl);
        }

        public String toString()
        {
          return "uploading part " + (partNumber + 1);
        }
      });
  }

  private ListenableFuture<Void> startPartUploadActual(final Upload upload,
                                                       final long position,
                                                       final OverallProgressListener opl)
  throws Exception
  {
    final int partNumber = (int) (position / chunkSize);
    final Cipher cipher;

    long partSize;
    if (encKeyName != null)
    {
      cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

      long preCryptSize = Math.min(fileLength - position, chunkSize);
      long blockSize = cipher.getBlockSize();
      partSize = blockSize * (preCryptSize/blockSize + 2);
    }
    else
    {
      cipher = null;
      partSize = Math.min(fileLength - position, chunkSize);
    }

    Callable<InputStream> inputStreamCallable = new Callable<InputStream>() {
      public InputStream call() throws Exception {
        FileInputStream fs = new FileInputStream(file);
        long skipped = fs.skip(position);
        while (skipped < position)
        {
          skipped += fs.skip(position - skipped);
        }

        BufferedInputStream bs = new BufferedInputStream(fs);
        InputStream in;
        if (cipher != null)
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

    return upload.uploadPart(partNumber, partSize, inputStreamCallable,
        Optional.fromNullable(opl));
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
    return executeWithRetry(_executor,
      new Callable<ListenableFuture<String>>()
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

  /**
   * Abort upload if something goes wrong
   */
  private AsyncFunction<Upload, Void> abortAsyncFunction()
  {
    return new AsyncFunction<Upload, Void>()
    {
      public ListenableFuture<Void> apply(Upload upload)
      {
        return abort(upload, 0);
      }
    };
  }

  /**
   * Execute abortActual with retry
   */
  private ListenableFuture<Void> abort(final Upload upload, final int
      retryCount)
  {
    return executeWithRetry(_executor,
        new Callable<ListenableFuture<Void>>() {
          public ListenableFuture<Void> call() {
            return abortActual(upload, retryCount);
          }

          public String toString() {
            return "aborting upload";
          }
        });
  }

  private ListenableFuture<Void> abortActual(final Upload upload,
                                             final int retryCount)
  {
    return upload.abort();
  }
}
