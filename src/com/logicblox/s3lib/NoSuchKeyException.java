package com.logicblox.s3lib;

class NoSuchKeyException extends Exception
{
  public NoSuchKeyException(String msg)
  {
    super(msg);
  }

  public NoSuchKeyException(Exception cause)
  {
    super(cause);
  }
}
