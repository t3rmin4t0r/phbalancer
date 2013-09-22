

for attempt in $(mapred job -list-attempt-ids $job MAP completed);
	do mapred job -logs $job $attempt | grep 'MapTask: Processing split' | sed "s/.*Paths:\(.*\)InputFormatClass.*/\1/" ; 
done
