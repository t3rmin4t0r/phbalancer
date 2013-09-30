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
import java.util.concurrent.*;

import org.notmysock.hdfs.RawProtocolWrapper.*;

public class PHBalancer extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new PHBalancer(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        CommandLineParser parser = new BasicParser();
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption("s","splits", true, "splits");
        options.addOption("p", "parallel", true, "parallel");
        options.addOption("x", "strategy", true, "strategy");
        options.addOption("n", "dryrun", true, "dryrun");
        CommandLine line = parser.parse(options, remainingArgs);

        if(!(line.hasOption("splits"))) {
          HelpFormatter f = new HelpFormatter();
          f.printHelp("PHBalancer", options);
          return 1;
        }
        
        File splits = new File(line.getOptionValue("splits"));
        int parallel = 1;
        boolean dryrun = line.hasOption("dryrun");

        if(line.hasOption("parallel")) {
          parallel = Integer.parseInt(line.getOptionValue("parallel"));
        }

        List<SplitReader.FutureSplit[]> parsed = SplitReader.parse(splits);
        FileSystem fs = FileSystem.get(conf);
        RawProtocolWrapper pw = new RawProtocolWrapper(fs);
        BalancerStrategy strategy = new LeastMoveStrategy(pw);

        for(FileSplit[] s: parsed) {
          ArrayList<BlockWithLocation> blocks = new ArrayList<BlockWithLocation>();
          for(FileSplit f: s) {
            blocks.addAll(Arrays.asList(pw.getLocations(f.getPath().toString(), f.getStart(), f.getLength())));
          }
          strategy.group(blocks.toArray(new BlockWithLocation[0]));
        }
        ScheduledMove[] plan = strategy.plan();

        ExecutorService pool = Executors.newFixedThreadPool(parallel);

        for(ScheduledMove mv: plan) {
          System.out.println(mv);
          if(!dryrun) {
            System.out.println("Moving " + mv);
            pool.submit((Runnable)mv);
          }
        }

        pool.shutdown();
        return 0;
    }
}
