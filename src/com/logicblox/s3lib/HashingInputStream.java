package com.logicblox.s3lib;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class HashingInputStream extends FilterInputStream
{
    private MessageDigest md;
    private byte[] digest;

    public HashingInputStream(InputStream in)
    {
        super(in);
        try
        {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            // No MD5, give up
            throw new RuntimeException(e);
        }
    }

    public byte[] getDigest()
    {
        if (digest == null)
        {
            digest = md.digest();
        }

        return digest;
    }

    @Override
    public int read() throws IOException
    {
        int res = in.read();
        if (res != -1)
        {
            md.update((byte) res);
        }
        return res;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        int count = in.read(b);
        if (count != -1)
        {
            md.update(b, 0, count);
        }
        return count;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        int count = in.read(b, off, len);
        if (count != -1)
        {
            md.update(b, off, count);
        }
        return count;
    }
}