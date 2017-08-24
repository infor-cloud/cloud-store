package com.logicblox.s3lib;


// Used to inject aborts for testing purposes
public class AbortInjection extends RuntimeException
{
  public AbortInjection(String msg)
  {
    super(msg);
  }

  public AbortInjection(String msg, Throwable cause)
  {
    super(msg, cause);
  }
}
