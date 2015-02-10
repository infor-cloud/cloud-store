package com.logicblox.s3lib;

class BadHashException extends Exception {
  public BadHashException(String msg)
  {
    super(msg);
  }
}
