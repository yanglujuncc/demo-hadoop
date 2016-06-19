
package ylj.demo.hadoop2;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;




public class DemoReducer extends Reducer<Text, LongWritable, Text, LongWritable>  {

	
	@Override
	public void setup(Context context) {
	
		Counter counter=context.getCounter("info", "reducer setup call");
		counter.increment(1);
	}
	
	@Override
	public void reduce( Text   key2, Iterable<LongWritable> value2s,Context context)
			throws IOException, InterruptedException {

		
		long counter=0;
		for(LongWritable value:value2s){
			counter+=value.get();
		}
		
		context.write(key2, new LongWritable(counter));

	}

}
