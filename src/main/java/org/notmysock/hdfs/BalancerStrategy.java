
package org.notmysock.hdfs;

import java.io.*;

import org.notmysock.hdfs.RawProtocolWrapper.*;

public abstract class BalancerStrategy {
  public abstract void group(BlockWithLocation[] blocks) throws IOException;
  public abstract ScheduledMove[] plan() throws IOException;
}
