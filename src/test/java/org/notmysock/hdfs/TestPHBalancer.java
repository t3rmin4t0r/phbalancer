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
import java.util.concurrent.*;

import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
import org.apache.hadoop.hdfs.security.token.block.*;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.token.*;

import org.apache.log4j.*;

import org.notmysock.hdfs.*;
import org.notmysock.hdfs.RawProtocolWrapper.*;

public class TestPHBalancer {
  private MiniDFSCluster cluster;
  private Configuration conf;
  private int nodes = 4;
  private String[] racks = {"/rack0", "/rack0", "/rack1", "/rack1"};
  private FileSystem fs;
  private byte[] buffer = "That trout lay shattered into a thousand fragments — I say a thousand, but they may have only been nine hundred.  I did not count them.".getBytes(); 

  private static Logger LOG = Logger.getLogger(TestPHBalancer.class);

  static {
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.WARN);
    rootLogger.addAppender(new ConsoleAppender(
               new PatternLayout("%-6r [%p] %c - %m%n")));
    LOG.setLevel(Level.INFO);
  }

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
    try {
      DFSTestUtil.waitReplication(fs, f, (short)3);
    } catch(InterruptedException ie) {
      ie.printStackTrace();
    } catch(TimeoutException te) {
      te.printStackTrace();
    }
  }

  @Test
  public void testBalancer() throws IOException {
    init();
    RawProtocolWrapper pw = new RawProtocolWrapper(fs);
    BalancerStrategy bs = new LeastMoveStrategy(pw);
    ArrayList<BlockWithLocation> blocks = new ArrayList<BlockWithLocation>();
    char[] abc = {'a', 'b', 'c'};
    for(char c: abc) {
      String path = String.format("/%c.txt", c);
      createFile(path);
      blocks.addAll(Arrays.asList(pw.getLocations(path, 0, buffer.length)));
    }
    bs.group(blocks.toArray(new BlockWithLocation[0]));
    int moves = 0;
    for(ScheduledMove mv: bs.plan()) {
      mv.dispatch();
      moves++;
    }

    Set<Datanode> hosts = null;
    for(char c: abc) {
      String path = String.format("/%c.txt", c);
      for(BlockWithLocation bloc: pw.getLocations(path, 0, buffer.length)) {
        if(hosts == null) {
          hosts = new HashSet<Datanode>();
          hosts.addAll(Arrays.asList(bloc.locations));
        } else {
          hosts.retainAll(Arrays.asList(bloc.locations));
        }
      }
    } 
    LOG.info("Balanced " + blocks.size() + " blocks in " + moves + " moves");
    if(hosts.size() < 2) {
      //one node per rack
      fail("LeastMoveBalancing failed to produce at least 2 nodes with data locality");
    }
  }

  @Test
  public void testMove() throws IOException {
    init();
    createFile("/a.txt");
    createFile("/b.txt");
    RawProtocolWrapper pw = new RawProtocolWrapper(fs);
    ArrayList<RawProtocolWrapper.BlockWithLocation> blocks = new ArrayList<RawProtocolWrapper.BlockWithLocation>();
    blocks.addAll(Arrays.asList(pw.getLocations("/a.txt", 0, buffer.length)));
    blocks.addAll(Arrays.asList(pw.getLocations("/b.txt", 0, buffer.length)));

    TreeSet<Datanode> hosts = new TreeSet<Datanode>(); 
    for(RawProtocolWrapper.BlockWithLocation block: blocks) {
      hosts.addAll(Arrays.asList(block.locations));
    }

    Datanode[] all = hosts.toArray(new Datanode[0]);

    for(RawProtocolWrapper.BlockWithLocation block: blocks) {
      List<Datanode> locations = Arrays.asList(block.locations);
      for(Datanode h: all) {
        if(locations.indexOf(h) == -1) {
          // move randomly, almost
          RawProtocolWrapper.ScheduledMove mv = pw.move(block, locations.get(0), h);
          mv.dispatch();
          break;
        }
      }
    }
  }
}
