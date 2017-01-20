package com.logicblox.s3lib;

import java.net.URL;
import java.net.MalformedURLException;


public class TestOptions
{
  private static String _host = "127.0.0.1";
  private static int _port = 9000;

  public static void parseArgs(String[] args)
  {
// TODO - add options for port and host
  }

  public static URL getEndpoint()
    throws MalformedURLException
  {
    return new URL("http", _host, _port, "/");
  }
}
