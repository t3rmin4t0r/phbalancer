package org.notmysock.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.*;

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

public class TestPHBalancer {
  private MiniDFSCluster cluster;
  private Configuration conf;
  private int nodes = 4;
  private String[] racks = {"/rack0", "/rack0", "/rack1", "/rack1"};

  @Before
  public void init() {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 16);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    try {
      cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(nodes)
                                .racks(racks)
                                .build();                        
      cluster.waitActive();
    } catch(IOException ioe) {
      ioe.printStackTrace();
    }
  }

  @After
  public void teardown() {
    cluster.shutdown();
    cluster = null;
    conf = null;
  }

  @Test
  public void testBalance() {
  }
}
