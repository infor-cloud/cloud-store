package com.logicblox.s3lib;

/**
 * A usage exception is thrown for incorrect usage of the
 * tool. Reporting a stack trace is not considered useful, and only
 * the message needs to be reported to the user/
 */
public class UsageException extends RuntimeException
{
  public UsageException(String msg)
  {
    super(msg);
  }

  public UsageException(String msg, Throwable cause)
  {
    super(msg, cause);
  }
}
