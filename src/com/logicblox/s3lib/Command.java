package com.logicblox.s3lib;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.security.Key;

public class Command
{
  protected File file;
  protected long chunkSize;
  protected Key encKey;
  protected Set<Integer> completedParts;
  protected long fileLength;

  protected static Key readKeyFromFile(String encKeyName, File encKeyFile) throws IOException, ClassNotFoundException
  {
    FileInputStream fs = new FileInputStream(encKeyFile);
    ObjectInputStream in = new ObjectInputStream(fs);
    Map<String,Key> keys = (HashMap<String,Key>) in.readObject();
    in.close();
    if (keys.containsKey(encKeyName)) {
      return keys.get(encKeyName);
    }
    return null;
  }
}
