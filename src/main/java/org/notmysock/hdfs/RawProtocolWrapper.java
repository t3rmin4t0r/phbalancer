package org.notmysock.hdfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.net.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;

import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
import org.apache.hadoop.hdfs.security.token.block.*;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.token.*;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.*;
import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;


import org.apache.commons.cli.*;
import org.apache.commons.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.net.*;
import java.math.*;
import java.security.*;

public class RawProtocolWrapper {
  final NamenodeProtocol namenode;
  final ClientProtocol client;
  final FileSystem fs;
  final String blockpoolID;
  final boolean isBlockTokenEnabled;
  final boolean encryptDataTransfer;
  final BlockTokenSecretManager blockTokenSecretManager;
  final DataEncryptionKey encryptionKey;
  final NetworkTopology cluster;

  public static class BlockWithLocation {
    public final String poolId; 
    public final long blkid;
    public final long len;
    public final long genstamp;
    public final boolean corrupt;
    public final Datanode[] locations;

    private BlockWithLocation(
      String poolId, 
      long blkid,
      long len,
      long genstamp,
      boolean corrupt,
      Datanode[] locations
    ) {
      this.poolId = poolId;
      this.blkid = blkid;
      this.len = len;
      this.genstamp = genstamp;
      this.corrupt = corrupt;
      this.locations = locations;
    }

    public String toString() {
      String[] hosts = new String[locations.length];
      int i = 0;
      for(Datanode dn: locations) {
        hosts[i++] = String.format("\"%s\"", dn.toString());
      }
      return String.format("\"%s:%d\": [%s]", poolId, blkid, StringUtils.join(",", hosts));
    }

    public ExtendedBlock getBlock() {
      return new ExtendedBlock(this.poolId, this.blkid, this.len, this.genstamp);
    }

    public static BlockWithLocation create(LocatedBlock lb) {
      DatanodeInfo[] nodes = lb.getLocations();
      Datanode[] locations = new Datanode[nodes.length];
      int i = 0;
      for(DatanodeInfo dn: nodes) {
        locations[i++] = new Datanode(dn);
      }
      long len = lb.getBlock().getNumBytes();
      boolean corrupt = lb.isCorrupt();
      String poolId = lb.getBlock().getBlockPoolId();
      long blkid = lb.getBlock().getBlockId();
      long genstamp = lb.getBlock().getGenerationStamp();

      return new BlockWithLocation(
          poolId, 
          blkid,
          len,
          genstamp,
          corrupt,
          locations
      );
    }
  }

  public static class Datanode implements Comparable<Datanode> {
    public final DatanodeInfo node;
    public Datanode(DatanodeInfo node) {
      this.node = node;
    }
    public String getRack() {
      return this.node.getNetworkLocation();
    }
    @Override
    public String toString() {
      return node.toString();
    }
    @Override
    public boolean equals(Object o1) {
      if(this == o1) {
        return true;
      }
      if(o1 instanceof Datanode) {
        return node.equals(((Datanode)(o1)).node);
      }
      return false;
    }
    @Override
    public int compareTo(Datanode o1) {
      return node.compareTo(o1.node);
    }
    @Override 
    public int hashCode() {
      return node.hashCode();
    }
  }

  public class ScheduledMove implements Runnable {
    public final BlockWithLocation block;
    public final DatanodeInfo src;
    public final DatanodeInfo dst;
    private DatanodeInfo proxy;
    public ScheduledMove(
      BlockWithLocation block,
      DatanodeInfo src,
      DatanodeInfo dst
    ) {
      this.block = block;
      this.src = src;
      this.dst = dst;
      this.proxy = findProxy();
    }

    public DatanodeInfo findProxy() {
      if(cluster.isOnSameRack(src, dst)) {
        return src;
      }
      /* try to move data from the same rack */
      for(Datanode h: block.locations) {
        if(dst.equals(h.node) || src.equals(h.node)) {
          continue;
        }
        if(cluster.isOnSameRack(dst, h.node)) {
          return h.node;
        }
      }
      // give up
      return src;
    }

    public void run() {
      System.out.println("Running " + this);
      dispatch();
    }

    public void dispatch() {
      Socket sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock.connect(
            NetUtils.createSocketAddr(dst.getXferAddr()),
            HdfsServerConstants.READ_TIMEOUT);
        sock.setKeepAlive(true);
        
        OutputStream unbufOut = sock.getOutputStream();
        InputStream unbufIn = sock.getInputStream();
        if (encryptionKey != null) {
          IOStreamPair encryptedStreams =
              DataTransferEncryptor.getEncryptedStreams(
                  unbufOut, unbufIn, encryptionKey);
          unbufOut = encryptedStreams.out;
          unbufIn = encryptedStreams.in;
        }
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.IO_FILE_BUFFER_SIZE));
        in = new DataInputStream(new BufferedInputStream(unbufIn,
            HdfsConstants.IO_FILE_BUFFER_SIZE));
        
        sendRequest(out);
        receiveResponse(in);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
      }
    }

     
    /* Send a block replace request to the output stream*/
    private void sendRequest(DataOutputStream out) throws IOException {
      final ExtendedBlock eb = block.getBlock();
      final Token<BlockTokenIdentifier> accessToken = getAccessToken(eb);
      new Sender(out).replaceBlock(eb, accessToken, src.getStorageID(), proxy);
    }
    
    /* Receive a block copy response from the input stream */ 
    private void receiveResponse(DataInputStream in) throws IOException {
      BlockOpResponseProto response = BlockOpResponseProto.parseFrom(
          vintPrefixed(in));
      if (response.getStatus() != Status.SUCCESS) {
        if (response.getStatus() == Status.ERROR_ACCESS_TOKEN) {
          throw new IOException("block move failed due to access token error");
        }
        throw new IOException("block move failed: " +
            response.getMessage());
      }
    }

    @Override
    public String toString() {
      return String.format("[%s] %s -> %s", block, src, dst);
    }

  }

  public RawProtocolWrapper(FileSystem fs) throws IOException {
    URI nn = FileSystem.getDefaultUri(fs.getConf());
    this.fs = fs;
    this.namenode =
      NameNodeProxies.createProxy(fs.getConf(), nn, NamenodeProtocol.class)
        .getProxy();
    this.client =
      NameNodeProxies.createProxy(fs.getConf(), nn, ClientProtocol.class)
        .getProxy();
    
    final NamespaceInfo namespaceinfo = namenode.versionRequest();
    this.blockpoolID = namespaceinfo.getBlockPoolID();

    final ExportedBlockKeys keys = namenode.getBlockKeys();
    this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
    if (isBlockTokenEnabled) {
      long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
      long blockTokenLifetime = keys.getTokenLifetime();
      String encryptionAlgorithm = fs.getConf().get(
          DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
      this.blockTokenSecretManager = new BlockTokenSecretManager(
          blockKeyUpdateInterval, blockTokenLifetime, blockpoolID,
          encryptionAlgorithm);
      this.blockTokenSecretManager.addKeys(keys);
    } else {
      this.blockTokenSecretManager = null;
    }
    this.encryptDataTransfer = fs.getServerDefaults(new Path("/"))
        .getEncryptDataTransfer();
    if (encryptDataTransfer) {
      this.encryptionKey = blockTokenSecretManager.generateDataEncryptionKey();
    } else {
      this.encryptionKey = null;
    }
    this.cluster = NetworkTopology.getInstance(fs.getConf());
  }

  /** Get an access token for a block. */
  private Token<BlockTokenIdentifier> getAccessToken(ExtendedBlock eb) 
    throws IOException {
    if (!isBlockTokenEnabled) {
      return BlockTokenSecretManager.DUMMY_TOKEN;
    } else {
      // todo: refresh tokens
      // blockTokenSecretManager.addKeys(namenode.getBlockKeys());
      return blockTokenSecretManager.generateToken(null, eb,
          EnumSet.of(BlockTokenSecretManager.AccessMode.REPLACE,
          BlockTokenSecretManager.AccessMode.COPY));
    }
  }

  public ScheduledMove move(BlockWithLocation block, Datanode src, Datanode dst) 
    throws IOException {
    return new ScheduledMove(block, src.node, dst.node);
  }

  public BlockWithLocation[] getLocations(String file, long start, long length) throws IOException {
    try {
      LocatedBlocks ll = client.getBlockLocations(file, start, length);
      List<LocatedBlock> blocks = ll.getLocatedBlocks();
      BlockWithLocation[] result = new BlockWithLocation[blocks.size()];      
      int i = 0;
      for(LocatedBlock b: blocks) {
        result[i++] = BlockWithLocation.create(b);
      }
      return result;
    } catch(Exception e) {
      throw new IOException(e);
    }
  }

  public Datanode[] getDataNodes() throws IOException {
    DatanodeInfo[] nodes = client.getDatanodeReport(HdfsConstants.DatanodeReportType.LIVE);
    Datanode[] dnodes = new Datanode[nodes.length];
    int i = 0;
    for(DatanodeInfo dn: nodes) {
      dnodes[i++] = new Datanode(dn);
    }
    return dnodes;
  }
}
