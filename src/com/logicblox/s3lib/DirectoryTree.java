package com.logicblox.s3lib;

import java.util.LinkedList;
import java.util.Queue;

public class DirectoryTree {
  
  public DirectoryNode root;
  
  public DirectoryTree() {
    this.root = new DirectoryNode(" ", " ", new Long(0), 0);
  }
  
  public void addElement(String elementValue, Long size) {
    String[] list = elementValue.split("/");
    
    root.addElement(root.rootPath, list, size, 0);
    
  }
  
  public void print(int depth) {
    Queue<DirectoryNode> queue = new LinkedList<DirectoryNode>();
    queue.clear();
    queue.add(this.root);
    while (! queue.isEmpty()) {
      DirectoryNode node = queue.remove();
      for (DirectoryNode n : node.childs) {
        System.out.print(n.toString() + "\n");
      }
      if (node.childs.get(0).depth == depth)
        break;
      queue.addAll(node.childs);
      
    }
  }
  
  public static void main(String[] args) {
    
    String slist[][] = new String[][] {
        {
            "/waad-testing", "50"
        }, {
            "/waad-testing/data/boo", "60"
        }, {
            "/waad-testing/foo", "10"
        }, {
            "/waad-testing/data/foo", "70"
        }
    };
    
    DirectoryTree tree = new DirectoryTree();
    for (String[] data : slist) {
      tree.addElement(data[0], Long.valueOf(data[1]));
    }
    tree.print(1);
  }
  
}
