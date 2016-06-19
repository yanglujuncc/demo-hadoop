/**
 *  @author hzyanglujun
 *  @version  创建时间:2016年3月4日 上午10:00:42
 */
package ylj.demo.hbase.MR;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.Durability;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author hzyanglujun
 *
 */
public class HBaseDemoOutputToHbaseJob2 extends Configured implements Tool {

	public static class HBaseDemoOutputToHbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			context.getCounter("UserInfo", "mapCall").increment(1);
			
			String[] fields = value.toString().split(",");

			context.getCounter("UserInfo", "fields.length=" + fields.length).increment(1);

			if (fields.length != 4) {
				return;
			}
			
			if (fields.length < 14) {
			//	continue;
			}

			// Extract each value
			context.getCounter("UserInfo", "row=" + fields[0]).increment(1);
			context.getCounter("UserInfo", "family=" + fields[1]).increment(1);
			context.getCounter("UserInfo", "qualifier=" + fields[2]).increment(1);
			context.getCounter("UserInfo", "value=" + fields[3]).increment(1);

			byte[] row = Bytes.toBytes(fields[0]);
			byte[] family = Bytes.toBytes(fields[1]);
			byte[] qualifier = Bytes.toBytes(fields[2]);
			byte[] cellV = Bytes.toBytes(fields[3]);
			
			// Create Put
			Put put = new Put(row);
			put.add(family, qualifier, cellV);
			put.setDurability(Durability.SKIP_WAL);
		
			context.getCounter("UserInfo", "put").increment(1);
			
			ImmutableBytesWritable   outkey = new ImmutableBytesWritable();
			outkey.set(row);
			
			context.write(outkey, put);
		}
	}

	

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
//		/conf.set("hbase.zookeeper.quorum", "hz-hbase3.photo.163.org,hz-hbase4.photo.163.org,hz-hbase5.photo.163.org");
	
		
		Path inputPath = new Path(args[0]);
		String tableName = args[1];

		System.out.println(" inputPath:"+inputPath);
		System.out.println(" tableName:"+tableName);
		
	    conf.set("zookeeper.znode.parent", "/hbase-sc");
        conf.set("hbase.zookeeper.quorum", "xxx,xxx,xxx");
  
        
		Job job = Job.getInstance(conf, "HBaseDemoOutputToHbaseJob2"); // 利用job取代了jobclient

		
		job.setJarByClass(HBaseDemoOutputToHbaseJob2.class);
		
		//set input
		FileInputFormat.setInputPaths(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);
		
		//set mapper
		job.setMapperClass(HBaseDemoOutputToHbaseMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		//set redurcer 
		TableMapReduceUtil.initTableReducerJob(tableName, null, job);
	
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new HBaseDemoOutputToHbaseJob2(), args);
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */

}
