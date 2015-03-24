package com.logicblox;

import java.io.File;
import java.net.URI;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.logicblox.s3lib.CloudStoreClient;
import com.logicblox.s3lib.S3Client;
import com.logicblox.s3lib.UploadOptions;
import com.logicblox.s3lib.UploadOptionsBuilder;
import com.logicblox.s3lib.UsageException;
import com.logicblox.s3lib.Utils;

public class S3uploader {

    static String encKeyName = "kiabi-encryption-key";
    static String cannedAcl = "bucket-owner-full-control";

    private static boolean isValidS3Acl(String value) {
        for (CannedAccessControlList acl : CannedAccessControlList.values()) {
            if (acl.toString().equals(value))
                return true;
        }
        return false;
    }

    public static void upload(CloudStoreClient client, UploadOptions options)
        throws Exception {

        if (options.getObjectKey().endsWith("/")) {
            URI url = client.getUri(options.getBucket(), options.getObjectKey());
            throw new UsageException("Destination key " + url + " should be " +
                "fully qualified. No trailing '/' is permitted.");
        }

        if (options.getFile().isFile()) {
            client.upload(options).get();
        } else if (options.getFile().isDirectory()) {
            client.uploadDirectory(options).get();
        } else {
            throw new UsageException("File '" + options.getFile() + "' is not" +
                " a file or a directory.");
        }
    }

    public static void main(String[] args) throws Exception {
        // Define the client configuration
        ClientConfiguration clientCfg = new ClientConfiguration();
        clientCfg.setProtocol(Protocol.HTTPS);
        // Add proxy connection if needed:
        //    clientCfg.setProxyHost("localhost");
        //    clientCfg.setProxyPort(8118);
        AmazonS3Client s3Client = new AmazonS3Client(clientCfg);

        // Encryption key directory.
        // String key_dir = Utils.getDefaultKeyDirectory()
        String key_dir = System.getProperty("user.home") + File.separator +
            ".s3lib-keys";

        // CloudStore client
        CloudStoreClient client = new S3Client(s3Client,
            Utils.getKeyProvider(key_dir));

        // Set upload options
        URI url = Utils.getURI("s3://kiabi-s3-poc/test/test.dat.gz");
        File file = new File("test.dat.gz");
        System.out.println(String.format("Encryption key is '%s'.",
            encKeyName));
        System.out.println(String.format("Uploading '%s' to '%s'.", file, url));

        if (!isValidS3Acl(cannedAcl))
            throw new UsageException("Unknown canned ACL '" + cannedAcl + "'");

        UploadOptions options = new UploadOptionsBuilder()
            .setFile(file)
            .setUri(url)
            .setEncKey(encKeyName)
            .setAcl(cannedAcl)
            .createUploadOptions();

        // Upload the file
        upload(client, options);
        client.shutdown();
    }


}
