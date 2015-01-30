package com.logicblox.s3lib;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.util.concurrent.*;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;


public class GCSUploadCommand extends Command {
    private String encKeyName;
    private String encryptedSymmetricKeyString;
    private String acl;

    private ListeningExecutorService _uploadExecutor;
    private ListeningScheduledExecutorService _executor;

    private String key;
    private String bucket;

    private boolean progress;

    public GCSUploadCommand(
            ListeningExecutorService uploadExecutor,
            ListeningScheduledExecutorService internalExecutor,
            File file,
            long chunkSize,
            String encKeyName,
            KeyProvider encKeyProvider,
            String acl,
            boolean progress)
            throws IOException {
        if (uploadExecutor == null)
            throw new IllegalArgumentException("non-null upload executor is required");
        if (internalExecutor == null)
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
            try {
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

        this.acl = acl;
        this.progress = progress;
    }

    /**
     * Run ties Step 1, Step 2, and Step 3 together. The return result is the ETag of the upload.
     */
    public ListenableFuture<S3File> run(final String bucket, final String key) throws FileNotFoundException {
        if (!file.exists())
            throw new FileNotFoundException(file.getPath());

        this.bucket = bucket;
        this.key = key;

        ListenableFuture<Upload> upload = startUpload(bucket, key);
        upload = Futures.transform(upload, startPartsAsyncFunction());
        ListenableFuture<String> result = Futures.transform(upload, completeAsyncFunction());
        return Futures.transform(result,
                new Function<String, S3File>() {
                    public S3File apply(String etag) {
                        S3File f = new S3File();
                        f.setLocalFile(file);
                        f.setETag(etag);
                        f.setBucketName(bucket);
                        f.setKey(key);
                        return f;
                    }
                });
    }

    /**
     * Step 1: Returns a future upload that is internally retried.
     */
    private ListenableFuture<Upload> startUpload(final String bucket, final String key) {
        return executeWithRetry(_executor,
                new Callable<ListenableFuture<Upload>>() {
                    public ListenableFuture<Upload> call() {
                        return startUploadActual(bucket, key);
                    }

                    public String toString() {
                        return "starting upload " + bucket + "/" + key;
                    }
                });
    }

    private ListenableFuture<Upload> startUploadActual(final String bucket, final String key) {
        UploadFactory factory = new GCSUploadFactory(getGCSClient(), _uploadExecutor, progress);

        Map<String, String> meta = new HashMap<String, String>();
        meta.put("s3tool-version", String.valueOf(Version.CURRENT));
        if (this.encKeyName != null) {
            meta.put("s3tool-key-name", encKeyName);
            meta.put("s3tool-symmetric-key", encryptedSymmetricKeyString);
        }
        // single-part => chunk size == file size
        // meta.put("s3tool-chunk-size", Long.toString(fileLength));
        // GCS "multipart" download seems to work fine
        meta.put("s3tool-chunk-size", Long.toString(chunkSize));
        meta.put("s3tool-file-length", Long.toString(fileLength));

        return factory.startUpload(bucket, key, meta, acl);
    }

    /**
     * Step 2: Upload parts
     */
    private AsyncFunction<Upload, Upload> startPartsAsyncFunction() {
        return new AsyncFunction<Upload, Upload>() {
            public ListenableFuture<Upload> apply(Upload upload) {
                return startParts(upload);
            }
        };
    }

    private ListenableFuture<Upload> startParts(final Upload upload) {
        ListenableFuture<Void> part = startPartUploadThread(upload);

        // we do not care about the voids, so we just return the upload
        // object.
        return Futures.transform(
                part,
                Functions.constant(upload));
    }

    private ListenableFuture<Void> startPartUploadThread(final Upload upload) {
        ListenableFuture<ListenableFuture<Void>> result =
                _executor.submit(new Callable<ListenableFuture<Void>>() {
                    public ListenableFuture<Void> call() throws Exception {
                        return GCSUploadCommand.this.startPartUpload(upload);
                    }
                });

        return Futures.dereference(result);
    }

    /**
     * Execute startPartUpload with retry
     */
    private ListenableFuture<Void> startPartUpload(final Upload upload) {
        final int partNumber = 0;

        return executeWithRetry(_executor,
                new Callable<ListenableFuture<Void>>() {
                    public ListenableFuture<Void> call() throws Exception {
                        return startPartUploadActual(upload);
                    }

                    public String toString() {
                        return "uploading part " + partNumber;
                    }
                });
    }

    private ListenableFuture<Void> startPartUploadActual(final Upload upload)
            throws Exception {
        final int partNumber = 0;
        final FileInputStream fs = new FileInputStream(file);

        BufferedInputStream bs = new BufferedInputStream(fs);
        InputStream in;
        long partSize;
        if (this.encKeyName != null) {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            in = new CipherWithInlineIVInputStream(bs, cipher, Cipher.ENCRYPT_MODE, encKey);

            long preCryptSize = fileLength;
            long blockSize = cipher.getBlockSize();
            partSize = blockSize * (preCryptSize / blockSize + 2);
        } else {
            in = bs;
            partSize = fileLength;
        }

        ListenableFuture<Void> uploadPartFuture = upload.uploadPart(partNumber, in, partSize);

        FutureFallback<Void> closeFile = new FutureFallback<Void>() {
            public ListenableFuture<Void> create(Throwable thrown) throws Exception {
                try {
                    fs.close();
                } catch (Exception e) {
                }

                return Futures.immediateFailedFuture(thrown);
            }
        };

        Futures.addCallback(uploadPartFuture, new FutureCallback<Void>() {
            public void onFailure(Throwable t) {
            }

            public void onSuccess(Void ignored) {
                try {
                    fs.close();
                } catch (Exception e) {
                }
            }
        });

        return Futures.withFallback(uploadPartFuture, closeFile);
    }

    /**
     * Step 3: Complete parts
     */
    private AsyncFunction<Upload, String> completeAsyncFunction() {
        return new AsyncFunction<Upload, String>() {
            public ListenableFuture<String> apply(Upload upload) {
                return complete(upload, 0);
            }
        };
    }

    /**
     * Execute completeActual with retry
     */
    private ListenableFuture<String> complete(final Upload upload, final int retryCount) {
        return executeWithRetry(_executor,
                new Callable<ListenableFuture<String>>() {
                    public ListenableFuture<String> call() {
                        return completeActual(upload, retryCount);
                    }

                    public String toString() {
                        return "completing upload";
                    }
                });
    }

    private ListenableFuture<String> completeActual(final Upload upload, final int retryCount) {
        return upload.completeUpload();
    }
}
