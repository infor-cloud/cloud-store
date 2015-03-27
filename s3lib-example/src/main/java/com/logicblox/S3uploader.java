package com.logicblox;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.google.common.util.concurrent.ListenableFuture;
import com.logicblox.s3lib.CloudStoreClient;
import com.logicblox.s3lib.S3Client;
import com.logicblox.s3lib.S3File;
import com.logicblox.s3lib.UploadOptions;
import com.logicblox.s3lib.UploadOptionsBuilder;
import com.logicblox.s3lib.UsageException;
import com.logicblox.s3lib.Utils;

public class S3uploader {

    public static void main(String[] args) throws URISyntaxException {
        CloudStoreClient client = getCloudStoreClient();
        UploadOptions options = getUploadOptions();

        // Upload the file/directory
        System.out.println(String.format("Uploading '%s' to '%s'.",
            options.getFile(), options.getObjectKey()));

        try {
            if (options.getObjectKey().endsWith("/"))
                throw new UsageException("Destination key " + options.getObjectKey()
                    + " should be fully qualified. No trailing '/' is permitted.");

            if (options.getFile().isFile()) {
                ListenableFuture<S3File> result = client.upload(options);
                S3File f = result.get();
            } else if (options.getFile().isDirectory()) {
                ListenableFuture<List<S3File>> result  =
                    client.uploadDirectory(options);
                List<S3File> fs = result.get();
            } else {
                throw new UsageException("File '" + options.getFile() + "' is not" +
                    " a file or a directory.");
            }
        } catch (UsageException e) {
            System.err.println(e.getMessage());
        } catch (ExecutionException e) {
            System.err.println(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.shutdown();
        }
    }

    private static CloudStoreClient getCloudStoreClient() {
        // Client configuration
        ClientConfiguration clientCfg = new ClientConfiguration();
        // Examples of configuration options:
        //    clientCfg.setProtocol(Protocol.HTTPS);
        //    clientCfg.setProxyHost("localhost");
        //    clientCfg.setProxyPort(8118);
        AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

        // Encryption key directory. Examples:
        //   String key_dir = Utils.getDefaultKeyDirectory()
        //   String key_dir = System.getProperty("user.home") + File.separator +
        //     ".s3lib-keys";
        String key_dir = null;
        assert key_dir != null;

        // CloudStore client
        return new S3Client(s3Client, Utils.getKeyProvider(key_dir));
    }

    private static UploadOptions getUploadOptions() throws URISyntaxException {
        // Set upload options

        // S3 object URI. Example:
        //   URI url = Utils.getURI("s3://bucket/dir/test.txt");
        URI url = null;
        assert url != null;

        // Save file to. Example:
        //   File file = new File("/path/to/test.txt");
        File file = null;
        assert file != null;

        // Encryption key-pair name. Example:
        //   String encKeyName = "enc-key";
        String encKeyName = null;

        // Permissions. Example:
        //   String cannedAcl = "bucket-owner-full-control";
        String cannedAcl = null;
        assert cannedAcl != null;

        if (!isValidS3Acl(cannedAcl))
            throw new UsageException("Unknown canned ACL '" + cannedAcl + "'");

        return new UploadOptionsBuilder()
            .setFile(file)
            .setUri(url)
            .setEncKey(encKeyName)
            .setAcl(cannedAcl)
            .createUploadOptions();
    }

    private static boolean isValidS3Acl(String value) {
        for (CannedAccessControlList acl : CannedAccessControlList.values()) {
            if (acl.toString().equals(value))
                return true;
        }
        return false;
    }
}
