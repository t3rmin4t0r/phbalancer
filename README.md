## Pigeonhole Balancer

To generate the splits file needed by phbalancer, run

    job = job_...
    for attempt in $(mapred job -list-attempt-ids $job MAP completed);
        do mapred job -logs $job $attempt | grep 'MapTask: Processing split' | sed "s/.*Paths:\(.*\)InputFormatClass.*/\1/" ; 
    done

This will generate a .splits file which can be used with the phbalancer as follows

    hadoop jar target/ph-balancer-*.jar -s query27.splits --strategy leastmove 

You can use the --dryrun option to test out the block placements.

And for balancing the dimension tables, you can do 

    hadoop jar target/ph-balancer-*.jar -s dimtables.txt --strategy concentrate -c 172.0.19.41:50010

