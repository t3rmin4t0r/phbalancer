package org.notmysock.hdfs;

import org.notmysock.hdfs.RawProtocolWrapper.*;

import java.io.*;
import java.util.*;

public class LeastMoveStrategy extends BalancerStrategy {
  private final RawProtocolWrapper protocols;
  private final ArrayList<BlockWithLocation[]> groups = new ArrayList<BlockWithLocation[]>();
  private final HashSet<Long> all = new HashSet<Long>();
  private final HashSet<Long> duplicates = new HashSet<Long>();

  private static final class FrequencyCounter<E> {
    private final HashMap<E, Long> hash = new HashMap<E, Long>();
    public void count(E obj) {
      Long old = hash.get(obj);
      if(old == null) {
        old = Long.valueOf(1);
      } else {
        old = Long.valueOf(old.longValue()+1);
      }
      hash.put(obj, old);
    }    

    public SortedMap<E, Long> result() {
      SortedMap<E,Long> map = new TreeMap<E,Long>(new FrequencyComparator<E>(hash));
      map.putAll(hash);
      return map;
    }
  }

  private static final class FrequencyComparator<E> implements Comparator<E> {
    Map<E, Long> base;
    public FrequencyComparator(Map<E, Long> base) {
      this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.
    public int compare(E a, E b) {
      if (base.get(a).compareTo(base.get(b)) >= 0) {
        return -1;
      } else {
        return 1;
      } // returning 0 would merge keys
    }
  }

  public LeastMoveStrategy(RawProtocolWrapper pw) {
    this.protocols = pw;
  }

  public void group(BlockWithLocation[] blocks) {
    groups.add(blocks);
  }

  private Datanode findMatch(Datanode dn, Set<Datanode> order, List<Datanode> others) {
    for(Datanode pref: order) {
      if(dn.equals(pref)) {
        // no move for this block
        return null;
      }
      if(dn.getRack().equals(pref.getRack())) {
        if(others.indexOf(pref) == -1) {
          return pref;
        }
      }
    }
    return null;
  }

  public ScheduledMove[] plan() throws IOException {
    ArrayList<ScheduledMove> moves = new ArrayList<ScheduledMove>(1024);
    for(BlockWithLocation[] blocks: groups) {
      FrequencyCounter<Datanode> counter = new FrequencyCounter<Datanode>();      
      for(BlockWithLocation b: blocks) {
        for(Datanode dn: b.locations) {
          counter.count(dn);
        }
        if(!all.add(b.blkid)) {
          duplicates.add(b.blkid);
        }
      }
      Set<Datanode> order = counter.result().keySet();
      for(BlockWithLocation b : blocks) {
        ArrayList<Datanode> locs = new ArrayList<Datanode>(Arrays.asList(b.locations));
        for(Datanode src: b.locations) {
          Datanode dst = findMatch(src, order, locs);
          if(dst != null) {
            moves.add(protocols.move(b, src, dst));
            locs.add(dst);
          } 
        }
      }
    }
    return moves.toArray(new ScheduledMove[0]);
  }
}
