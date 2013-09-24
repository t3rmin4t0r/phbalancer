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

import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
import org.apache.hadoop.hdfs.security.token.block.*;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.token.*;

public class TestPHBalancer {
  private MiniDFSCluster cluster;
  private Configuration conf;
  private int nodes = 4;
  private String[] racks = {"/rack0", "/rack0", "/rack1", "/rack1"};
  private FileSystem fs;
  private byte[] buffer = "That trout lay shattered into a thousand fragments—I say a thousand, but they may have only been nine hundred.  I did not count them.".getBytes(); 

  @Before
  public void init() throws IOException {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 16);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(nodes)
                                .racks(racks)
                                .build();                                
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
  public void teardown() throws IOException {
    fs.close();
    cluster.shutdown();
    cluster = null;
    conf = null;
  }

  public void createFile(String name) throws IOException {
    Path f = new Path(name);
    FSDataOutputStream out = fs.create(f);
    out.write(buffer);
    out.close();  
  }

  @Test
  public void testBalance() throws IOException {
    createFile("/a.txt");
    createFile("/b.txt");
    RawProtocolWrapper pw = new RawProtocolWrapper(fs);
    ArrayList<RawProtocolWrapper.BlockWithLocation> blocks = new ArrayList<RawProtocolWrapper.BlockWithLocation>();
    blocks.addAll(Arrays.asList(pw.getLocations("/a.txt", 0, buffer.length)));
    blocks.addAll(Arrays.asList(pw.getLocations("/b.txt", 0, buffer.length)));

    TreeSet<DatanodeInfo> hosts = new TreeSet<DatanodeInfo>(); 
    for(RawProtocolWrapper.BlockWithLocation block: blocks) {
      System.out.println(block.toString());
      hosts.addAll(Arrays.asList(block.locations));
    }

  }
}
