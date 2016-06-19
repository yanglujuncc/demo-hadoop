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
public class HBaseDemoOutputToHbaseJob extends Configured implements Tool {

	public static class HBaseDemoOutputToHbaseMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			context.write(new LongWritable(key.get()), new Text(value.toString()));
		}
	}

	public static class HBaseDemoOutputToHbaseReducer extends TableReducer<LongWritable, Text, ImmutableBytesWritable> {

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			context.getCounter("UserInfo", "redurceCall").increment(1);

			for (Text val : values) {// 遍历求和

				String[] fields = val.toString().split(",");

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
				byte[] value = Bytes.toBytes(fields[3]);
				
				// Create Put
				Put put = new Put(row);
				
				put.add(family, qualifier, value);
				// Uncomment below to disable WAL. This will improve performance
				// but
				// means you will experience data loss in the case of a
				put.setDurability(Durability.SKIP_WAL);
			
				
				context.getCounter("UserInfo", "put").increment(1);

				
				try {
					context.write(null, put);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

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
        conf.set("hbase.zookeeper.quorum", "xxx,xxxx,xxxx");
  
        
		Job job = Job.getInstance(conf, "HBaseDemoOutputToHbaseJob"); // 利用job取代了jobclient

		
		job.setJarByClass(HBaseDemoOutputToHbaseJob.class);
		
		//set input
		FileInputFormat.setInputPaths(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);
		
		//set mapper
		job.setMapperClass(HBaseDemoOutputToHbaseMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		//set redurcer 
		TableMapReduceUtil.initTableReducerJob(tableName, HBaseDemoOutputToHbaseReducer.class, job);
	
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new HBaseDemoOutputToHbaseJob(), args);
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */

}
