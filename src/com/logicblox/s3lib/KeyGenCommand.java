package com.logicblox.s3lib;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.*;

public class KeyGenCommand {

    private KeyPairGenerator keypairGen;
    private KeyPair keypair;
    private PublicKey publickey;
    private PrivateKey privateKey;

    public KeyGenCommand(String algo, int nbits)
            throws NoSuchAlgorithmException {
        keypairGen = KeyPairGenerator.getInstance(algo);
        keypairGen.initialize(nbits);
        keypair = keypairGen.generateKeyPair();
        publickey = keypair.getPublic();
        privateKey = keypair.getPrivate();
    }

    public void savePemKeypair(String fn) throws IOException {
        String tmpfn = fn + "~";
        String pem = getPemPublicKey() + "\n" + getPemPrivateKey();

        File tmpf = new File(tmpfn);
        FileOutputStream fos = new FileOutputStream(tmpf);
        DataOutputStream dos = new DataOutputStream(fos);
        dos.writeBytes(pem);
        dos.flush();
        dos.close();

        File pemf = new File(fn);
        FileUtils.moveFile(tmpf, pemf);
    }

    public String getPemPrivateKey() {
        String encoded = b64encode(privateKey.getEncoded());
        encoded = "-----BEGIN PRIVATE KEY-----\n" + encoded + "-----END PRIVATE KEY-----\n";

        return encoded;
    }

    public String getPemPublicKey() {
        String encoded = b64encode(publickey.getEncoded());
        encoded = "-----BEGIN PUBLIC KEY-----\n" + encoded + "-----END PUBLIC KEY-----\n";

        return encoded;
    }

    private String b64encode(byte[] keyBytes) {
        Base64 b64 = new Base64(Base64.PEM_CHUNK_SIZE);
        return b64.encodeToString(keyBytes);
    }

}