package org.notmysock.hdfs;

import org.notmysock.hdfs.RawProtocolWrapper.*;

import java.io.*;
import java.util.*;

public class ConcentrateStrategy extends BalancerStrategy {
  private final RawProtocolWrapper protocols;
  private final ArrayList<BlockWithLocation[]> groups = new ArrayList<BlockWithLocation[]>();
  private final HashSet<Long> all = new HashSet<Long>();
  private final HashSet<Long> duplicates = new HashSet<Long>();
  private final List<Datanode> targets = new ArrayList<Datanode>();

  public ConcentrateStrategy(RawProtocolWrapper pw, String[] hosts) throws IOException {
    this.protocols = pw;
    Datanode[] nodes = pw.getDataNodes();
    for(Datanode dn: nodes) {
      for(String host: hosts) {
        if(host.equals(dn.toString())) {
          targets.add(dn);
          break;
        }
      }
    }
  }

  public void group(BlockWithLocation[] blocks) {
    groups.add(blocks);
  }

  private Datanode findMatch(Datanode dn, Set<Datanode> order, List<Datanode> others) {
    return null;
  }

  public ScheduledMove[] plan() throws IOException {
    ArrayList<ScheduledMove> moves = new ArrayList<ScheduledMove>(1024);
    for(BlockWithLocation[] blocks: groups) {
      for(BlockWithLocation block: blocks) {
        int i = 0;
        HashSet<Datanode> sources = new HashSet<Datanode>();
        sources.addAll(Arrays.asList(block.locations));
        sources.removeAll(targets);
        // move from sources to targets
        for(Datanode dn: sources) {
          if(i >= targets.size()) {
            break;
          }
          if(targets.indexOf(dn) == -1) {
            moves.add(protocols.move(block, dn, targets.get(i)));
            i++;
          }
        }
      }
    }
    return moves.toArray(new ScheduledMove[0]);
  }
}
