package ylj.demo.hadoop2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;



public class DemoPartitioner extends Partitioner<Text,LongWritable>{

    @Override
    public int getPartition(Text key, LongWritable counter, int numPartitions) {
    	
    	//
    	String keyString=key.toString();
    	
    	return (keyString.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }


}
