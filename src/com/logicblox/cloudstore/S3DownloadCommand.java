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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

class S3DownloadCommand
  extends Command
{
  private DownloadOptions _options;
  private KeyProvider _encKeyProvider;
  private boolean _dryRun;
  private ConcurrentMap<Integer, byte[]> _etags = new ConcurrentSkipListMap<>();
  private OverallProgressListenerFactory _progressListenerFactory;

  public S3DownloadCommand(DownloadOptions options)
    throws IOException
  {
    super(options);
    _options = options;
    _encKeyProvider = _client.getKeyProvider();
    _dryRun = _options.isDryRun();

    this.file = _options.getFile();
    createNewFile();
    _progressListenerFactory = _options.getOverallProgressListenerFactory().orElse(null);
  }

  private void createNewFile()
    throws IOException
  {
    file = file.getAbsoluteFile();
    File dir = file.getParentFile();
    if(!dir.exists())
    {
      List<File> newDirs = Utils.mkdirs(dir, _dryRun);
      if(_dryRun)
      {
        for(File f : newDirs)
          System.out.println("<DRYRUN> creating missing directory '" + f.getAbsolutePath() + "'");
      }
    }

    if(file.exists())
    {
      if(_options.doesOverwrite())
      {
        if(_dryRun)
        {
          System.out.println("<DRYRUN> overwrite existing file '" + file.getAbsolutePath() + "'");
        }
        else
        {
          if(!file.delete())
          {
            throw new UsageException("Could not delete existing file '" + file + "'");
          }
        }
      }
      else
      {
        throw new UsageException(
          "File '" + file + "' already exists.  Please delete or use --overwrite");
      }
    }

    if(!_dryRun)
    {
      if(!file.createNewFile())
      {
        throw new IOException("File '" + file + "' already exists");
      }
    }
  }


  public ListenableFuture<StoreFile> run()
  {
    if(_dryRun)
    {
      System.out.println(
        "<DRYRUN> downloading '" + getUri(_options.getBucketName(), _options.getObjectKey()) +
          "' to '" + this.file.getAbsolutePath() + "'");
      return Futures.immediateFuture(null);
    }

    ExistsOptions opts = _client.getOptionsBuilderFactory()
      .newExistsOptionsBuilder()
      .setBucketName(_options.getBucketName())
      .setObjectKey(_options.getObjectKey())
      .createOptions();

    ListenableFuture<Metadata> existsFuture = _client.exists(opts);
    ListenableFuture<StoreFile> result = Futures.transform(existsFuture,
      new AsyncFunction<Metadata, StoreFile>()
      {
        public ListenableFuture<StoreFile> apply(Metadata mdata)
          throws UsageException
        {
          if(null == mdata)
          {
            throw new UsageException(
              "Object not found at " + getUri(_options.getBucketName(), _options.getObjectKey()));
          }
          return scheduleExecution();
        }
      });

    return result;
  }


  private ListenableFuture<StoreFile> scheduleExecution()
  {
    ListenableFuture<S3Download> download = startDownload();
    download = Futures.transform(download, startPartsAsyncFunction());
    download = Futures.transform(download, validate());
    ListenableFuture<StoreFile> res = Futures.transform(download,
      new Function<S3Download, StoreFile>()
      {
        public StoreFile apply(S3Download download)
        {
          StoreFile f = new StoreFile();
          f.setLocalFile(S3DownloadCommand.this.file);
          f.setETag(download.getETag());
          f.setBucketName(_options.getBucketName());
          f.setObjectKey(_options.getObjectKey());
          return f;
        }
      });

    return Futures.withFallback(res, new FutureFallback<StoreFile>()
    {
      public ListenableFuture<StoreFile> create(Throwable t)
      {
        if(S3DownloadCommand.this.file.exists())
        {
          S3DownloadCommand.this.file.delete();
        }

        if(t instanceof UsageException)
        {
          return Futures.immediateFailedFuture(t);
        }
        return Futures.immediateFailedFuture(new Exception(
          "Error " + "downloading " + getUri(_options.getBucketName(), _options.getObjectKey()) +
            ".", t));
      }
    });
  }

  /**
   * Step 1: Start download and fetch metadata.
   */
  private ListenableFuture<S3Download> startDownload()
  {
    return executeWithRetry(_client.getInternalExecutor(),
      new Callable<ListenableFuture<S3Download>>()
      {
        public ListenableFuture<S3Download> call()
        {
          return startDownloadActual();
        }

        public String toString()
        {
          String toStringOutput = "starting download " + _options.getBucketName() + "/" +
            _options.getObjectKey();
          if(_options.getVersion().isPresent())
          {
            return toStringOutput + " version id = " + _options.getVersion().get();
          }
          return toStringOutput;
        }
      });
  }

  private ListenableFuture<S3Download> startDownloadActual()
  {
    AsyncFunction<S3Download, S3Download> initDownload = new AsyncFunction<S3Download, S3Download>()
    {
      public ListenableFuture<S3Download> apply(S3Download download)
      {
        Map<String, String> meta = download.getMeta();

        String errPrefix = getUri(download.getBucketName(), download.getObjectKey()) + ": ";
        long len = download.getLength();
        long cs = chunkSize;
        if(meta.containsKey("s3tool-version"))
        {
          String objectVersion = meta.get("s3tool-version");

          if(!String.valueOf(Version.CURRENT).equals(objectVersion))
          {
            throw new UsageException(
              errPrefix + "file uploaded with unsupported version: " + objectVersion +
                ", should be " + Version.CURRENT);
          }
          if(meta.containsKey("s3tool-key-name"))
          {
            if(_encKeyProvider == null)
            {
              throw new UsageException(errPrefix + "No encryption key provider is specified");
            }
            String keyName = meta.get("s3tool-key-name");
            String keyNamesStr = meta.get("s3tool-key-name");
            List<String> keyNames = new ArrayList<>(Arrays.asList(keyNamesStr.split(",")));
            String symKeyStr;
            PrivateKey privKey = null;
            if(keyNames.size() == 1)
            {
              // We handle objects with a single encryption key separately
              // because it's allowed not to have "s3tool-pubkey-hash" header
              // (for backwards compatibility)
              try
              {
                privKey = _encKeyProvider.getPrivateKey(keyName);
                if(meta.containsKey("s3tool-pubkey-hash"))
                {
                  String pubKeyHashHeader = meta.get("s3tool-pubkey-hash");
                  PublicKey pubKey = Command.getPublicKey(privKey);
                  String pubKeyHashLocal = DatatypeConverter.printBase64Binary(
                    DigestUtils.sha256(pubKey.getEncoded())).substring(0, 8);

                  if(!pubKeyHashLocal.equals(pubKeyHashHeader))
                  {
                    throw new UsageException(
                      "Public-key checksums do not match. " + "Calculated hash: " +
                        pubKeyHashLocal + ", Expected hash: " + pubKeyHashHeader);
                  }
                }
              }
              catch(NoSuchKeyException e)
              {
                throw new UsageException(
                  errPrefix + "private key '" + keyName + "' is not available to decrypt");
              }
              symKeyStr = meta.get("s3tool-symmetric-key");
            }
            else
            {
              // Objects with multiple encryption keys must have
              // "s3tool-pubkey-hash" header. We might want to relax this
              // requirement.
              if(!meta.containsKey("s3tool-pubkey-hash"))
              {
                throw new UsageException(
                  errPrefix + " public key hashes are " + "required when object has multiple " +
                    "encryption keys");
              }
              String pubKeyHashHeadersStr = meta.get("s3tool-pubkey-hash");
              List<String> pubKeyHashHeaders = new ArrayList<>(
                Arrays.asList(pubKeyHashHeadersStr.split(",")));
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
                  String pubKeyHashLocal = DatatypeConverter.printBase64Binary(
                    DigestUtils.sha256(pubKey.getEncoded())).substring(0, 8);

                  if(pubKeyHashLocal.equals(pubKeyHashHeaders.get(privKeyIndex)))
                  {
                    // Successfully-read, validated key.
                    privKeyFound = true;
                    break;
                  }
                }
                catch(NoSuchKeyException e)
                {
                  throw new UsageException(
                    errPrefix + "Cannot generate the " + "public key out of the private one" +
                      " for " + kn);
                }
              }

              if(privKey == null || !privKeyFound)
              {
                // No private key found
                throw new UsageException(errPrefix + "No eligible private key" + " found");
              }
              List<String> symKeys = new ArrayList<>(
                Arrays.asList(meta.get("s3tool-symmetric-key").split(",")));
              symKeyStr = symKeys.get(privKeyIndex);
            }

            Cipher cipher;
            byte[] encKeyBytes;
            try
            {
              cipher = Cipher.getInstance("RSA");
              cipher.init(Cipher.DECRYPT_MODE, privKey);
              encKeyBytes = cipher.doFinal(DatatypeConverter.parseBase64Binary(symKeyStr));
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

            encKey = new SecretKeySpec(encKeyBytes, "AES");
          }

          cs = Long.valueOf(meta.get("s3tool-chunk-size"));
          len = Long.valueOf(meta.get("s3tool-file-length"));
        }

        setFileLength(len);
        if(cs == 0)
        {
          cs = Utils.getDefaultChunkSize(len);
        }
        setChunkSize(cs);
        return Futures.immediateFuture(download);
      }
    };
    S3DownloadFactory factory = new S3DownloadFactory(_options, getS3Client(),
      _client.getApiExecutor());
    return Futures.transform(factory.startDownload(), initDownload);
  }

  /**
   * Step 2: Start downloading parts
   */
  private AsyncFunction<S3Download, S3Download> startPartsAsyncFunction()
  {
    return new AsyncFunction<S3Download, S3Download>()
    {
      public ListenableFuture<S3Download> apply(S3Download download)
        throws Exception
      {
        return startParts(download);
      }
    };
  }

  private ListenableFuture<S3Download> startParts(S3Download download)
    throws IOException, UsageException
  {
    OverallProgressListener opl = null;
    if(_progressListenerFactory != null)
    {
      opl = _progressListenerFactory.create(
        new ProgressOptionsBuilder().setObjectUri(getUri(download.getBucketName(), download.getObjectKey()))
          .setOperation("download")
          .setFileSizeInBytes(fileLength)
          .createProgressOptions());
    }

    List<ListenableFuture<Integer>> parts = new ArrayList<ListenableFuture<Integer>>();
    for(long position = 0; position < fileLength || (position == 0 && fileLength == 0);
        position += chunkSize)
    {
      parts.add(startPartDownload(download, position, opl));
    }

    return Futures.transform(Futures.allAsList(parts), Functions.constant(download));
  }

  private ListenableFuture<Integer> startPartDownload(
    final S3Download download, final long position, final OverallProgressListener opl)
  {
    final int partNumber = (int) (position / chunkSize);

    return executeWithRetry(_client.getInternalExecutor(), new Callable<ListenableFuture<Integer>>()
    {
      public ListenableFuture<Integer> call()
      {
        return startPartDownloadActual(download, position, opl);
      }

      public String toString()
      {
        return "downloading part " + (partNumber + 1);
      }
    });
  }

  private ListenableFuture<Integer> startPartDownloadActual(
    final S3Download download, final long position, OverallProgressListener opl)
  {
    final int partNumber = (int) (position / chunkSize);
    long start;
    long partSize;

    if(encKey != null)
    {
      long blockSize;
      try
      {
        blockSize = Cipher.getInstance("AES/CBC/PKCS5Padding").getBlockSize();
      }
      catch(NoSuchAlgorithmException e)
      {
        throw new RuntimeException(e);
      }
      catch(NoSuchPaddingException e)
      {
        throw new RuntimeException(e);
      }

      long postCryptSize = Math.min(fileLength - position, chunkSize);
      start = partNumber * blockSize * (chunkSize / blockSize + 2);
      partSize = blockSize * (postCryptSize / blockSize + 2);
    }
    else
    {
      start = position;
      partSize = Math.min(fileLength - position, chunkSize);
    }

    ListenableFuture<InputStream> getPartFuture = download.getPart(start, start + partSize - 1,
      opl);

    AsyncFunction<InputStream, Integer> readDownloadFunction
      = new AsyncFunction<InputStream, Integer>()
    {
      public ListenableFuture<Integer> apply(InputStream stream)
        throws Exception
      {
        try
        {
          readDownload(download, stream, position, partNumber);
          return Futures.immediateFuture(partNumber);
        }
        finally
        {
          // make sure that the stream is always closed
          try
          {
            stream.close();
          }
          catch(IOException e)
          {
          }
        }
      }
    };

    return Futures.transform(getPartFuture, readDownloadFunction);
  }

  private void readDownload(
    S3Download download, InputStream inStream, long position, int partNumber)
    throws Exception
  {
    HashingInputStream stream = new HashingInputStream(inStream);
    RandomAccessFile out = new RandomAccessFile(file, "rw");
    out.seek(position);

    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

    InputStream in;
    if(encKey != null)
    {
      in = new CipherWithInlineIVInputStream(stream, cipher, Cipher.DECRYPT_MODE, encKey);
    }
    else
    {
      in = stream;
    }

    int postCryptSize = (int) Math.min(fileLength - position, chunkSize);
    int bufSize = 8192;
    int offset = 0;
    byte[] buf = new byte[bufSize];

    Runnable cleanup = () -> {
      try
      {
        out.close();
        stream.close();
      }
      catch(IOException ignored)
      {
      }
    };

    // Handle empty encrypted file, offset == postCryptSize is implied
    if(encKey != null && postCryptSize == 0)
    {
      int result = readSafe(in, buf, 0, 0, cleanup);
      if(result != -1)
      {
        // TODO: Check if the correct/expected result here should be 0 (instead
        // of -1).
        cleanup.run();
        throw new IOException("EOF was expected");
      }
    }
    else // Not necessary, just for easier reading
    {
      while(offset < postCryptSize)
      {
        int len = Math.min(bufSize, postCryptSize - offset);
        int result = readSafe(in, buf, 0, len, cleanup);
        if(result == -1)
        {
          cleanup.run();
          throw new IOException("unexpected EOF");
        }

        writeSafe(out, buf, 0, result, cleanup);
        offset += result;
      }
    }

    cleanup.run();
    _etags.put(partNumber, stream.getDigest());
  }

  private int readSafe(InputStream in, byte[] buf, int offset, int len, Runnable cleanup)
    throws IOException
  {
    int result;
    try
    {
      result = in.read(buf, offset, len);
    }
    catch(IOException e)
    {
      cleanup.run();
      throw e;
    }
    return result;
  }

  private void writeSafe(RandomAccessFile out, byte[] buf, int offset, int len, Runnable cleanup)
    throws IOException
  {
    try
    {
      out.write(buf, offset, len);
    }
    catch(IOException e)
    {
      cleanup.run();
      throw e;
    }
  }

  private AsyncFunction<S3Download, S3Download> validate()
  {
    return new AsyncFunction<S3Download, S3Download>()
    {
      public ListenableFuture<S3Download> apply(S3Download download)
      {
        return validateChecksum(download);
      }
    };
  }

  private ListenableFuture<S3Download> validateChecksum(final S3Download download)
  {
    ListenableFuture<S3Download> result = _client.getInternalExecutor()
      .submit(new Callable<S3Download>()
      {
        public S3Download call()
          throws Exception
        {
          String remoteEtag = download.getETag();
          String localDigest = "";
          String fn = "'s3://" + download.getBucketName() + "/" + download.getObjectKey() + "'";

          if(null == remoteEtag)
          {
            System.err.println(
              "Warning: Skipped checksum validation for " + fn + ".  No etag attached to object.");
            return download;
          }

          if((remoteEtag.length() > 32) && (remoteEtag.charAt(32) == '-'))
          {
            // Object has been uploaded using S3's multipart upload protocol,
            // so it has a special Etag documented here:
            // http://permalink.gmane.org/gmane.comp.file-systems.s3.s3tools/583

            // Since, the Etag value depends on the value of the chuck size (and
            // the number of the chucks) we cannot validate robustly checksums for
            // files uploaded with tool other than s3tool.
            Map<String, String> meta = download.getMeta();
            if(!meta.containsKey("s3tool-version"))
            {
              System.err.println(
                "Warning: Skipped checksum " + "validation for " + fn + ". It was uploaded using " +
                  "other tool's multipart protocol.");
              return download;
            }

            int expectedPartsNum = fileLength == 0 ? 1
              : (int) Math.ceil(fileLength / (double) chunkSize);
            int actualPartsNum = Integer.parseInt(remoteEtag.substring(33));

            if(expectedPartsNum != actualPartsNum)
            {
              System.err.println(
                "Warning: Skipped checksum validation for " + fn + ". Actual number of parts: " +
                  actualPartsNum + ", Expected number of parts: " + expectedPartsNum +
                  ". Probably the ETag was changed by using another tool.");
              return download;
            }

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            for(Integer pNum : _etags.keySet())
            {
              os.write(_etags.get(pNum));
            }

            localDigest = DigestUtils.md5Hex(os.toByteArray()) + "-" + _etags.size();
          }
          else
          {
            // Object has been uploaded using S3's simple upload protocol,
            // so its Etag should be equal to object's MD5.
            // Same should hold for objects uploaded to GCS (if "compose" operation
            // wasn't used).
            if(_etags.size() == 1)
            {
              // Single-part download (1 range GET).
              localDigest = DatatypeConverter.printHexBinary(_etags.get(0)).toLowerCase();
            }
            else
            {
              // Multi-part download (>1 range GETs).
              System.err.println("Warning: Skipped checksum validation for " + fn +
                ". No efficient way to compute MD5 on multipart downloads of files with " +
                "singlepart ETag.");
              return download;
            }
          }
          if(remoteEtag.equals(localDigest))
          {
            return download;
          }
          else
          {
            throw new BadHashException(
              "Failed checksum validation for " + download.getBucketName() + "/" + download.getObjectKey() +
                ". " + "Calculated MD5: " + localDigest + ", Expected MD5: " + remoteEtag);
          }
        }
      });

    return result;
  }
}
