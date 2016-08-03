package com.logicblox.s3lib;

import java.util.ArrayList;
import java.util.List;

public class DirectoryNode {
  
  List<DirectoryNode> childs;
  String fileName;
  long size;
  
  public DirectoryNode(long size, String fileName) {
    childs = new ArrayList<DirectoryNode>();
    this.fileName = fileName;
    this.size = size;
  }
  
}
