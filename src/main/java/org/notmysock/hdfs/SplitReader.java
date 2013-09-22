package org.notmysock.hdfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;

import org.apache.commons.cli.*;
import org.apache.commons.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.net.*;
import java.math.*;
import java.security.*;


public class SplitReader { 

  public static class FutureSplit extends FileSplit {
    public FutureSplit(Path file, long start, long length) {
      super(file, start, length, null);
    }

    private String[] hosts = null;

    @Override
    public String[] getLocations() throws IOException {
      return hosts;
    }

    public void setLocations(String[] hosts) throws IOException {
      this.hosts = hosts;
    }

    public static FutureSplit parse(String formatted) {
      String[] components = formatted.split("[:+]");
      Path file;
      long start = 0;
      long length = -1;
      switch(components.length) {
        case 3: {
          length = Long.valueOf(components[2]);          
        }
        // fall through no-break
        case 2: {
          start = Long.valueOf(components[1]);
        }
        // fall through no-break
        case 1: {
          return new FutureSplit(new Path(components[0]), start, length);
        }
        // returned already
      }
      return null;
    }
  }

  public static List<FutureSplit[]> parse(File input) 
    throws IOException {
    ArrayList<FutureSplit[]> splits = new ArrayList<FutureSplit[]>();
    BufferedReader reader = new BufferedReader(new FileReader(input));
    String line = null;
    while((line = reader.readLine()) != null) {
      splits.add(parseSplit(line));
    }
    return splits;
  }

  private static FutureSplit[] parseSplit(String line) {
    String[] tokens = line.split(","); // performance?
    FutureSplit[] splits = new FutureSplit[tokens.length];
    for(int i = 0; i < tokens.length; i++) {
      splits[i] = FutureSplit.parse(tokens[i]);
    }
    return splits;
  }
}
