/**
 *  @author hzyanglujun
 *  @version  创建时间:2016年3月4日 上午10:00:42
 */
package ylj.demo.hbase.MR;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author hzyanglujun
 *
 */
public class HBaseDemoInputFromHbaseJob extends Configured implements Tool {
	
	public static class HBaseDemoInputFromHbaseJobMapper extends TableMapper<Text, Text >  {
		
		@Override
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
	
			String key=new String(value.getRow());
		
			NavigableMap<byte[], NavigableMap<byte[],  byte[]>> map=value.getNoVersionMap();
			
			Map<String, Map<String, String>> strMap= toStrMap(map );
			
        	context.write(new Text(key), new Text(strMap.toString()));
		}
		
		public  static	Map<String, Map<String, String>> toStrMap(NavigableMap<byte[], NavigableMap<byte[], byte[]>> byteMapMap ){
			
			Map<String, Map<String, String>> strMapMap = new HashMap<String, Map<String, String>>();
			for (Entry<byte[], NavigableMap<byte[], byte[]>> entry :byteMapMap.entrySet()) {
				
				NavigableMap<byte[], byte[]>  byteMap=entry.getValue();
				Map<String, String>  strMap= new HashMap<String, String>(); 
				for(Entry<byte[], byte[]> entry2 :byteMap.entrySet()){
					strMap.put(new String(entry2.getKey()),new String( entry2.getValue()));
				}
				
				strMapMap.put(new String(entry.getKey()), strMap);
			}
			
			return strMapMap;
		}
	}
	
	
	

	public static class HBaseDemoInputFromHbaseJobReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key2, Iterable<Text> value2s, Context context)
				throws IOException, InterruptedException {
			
			String totalV="";
			for (Text value : value2s) {
				totalV += value.toString();
			}

			context.write(new Text(key2.toString()), new Text(totalV));
		}
	}
	
	public Scan getScan(){
		Scan aScan=new Scan();
		
		aScan.setStartRow("3".getBytes());
		aScan.setStopRow("5".getBytes());
		
		
		return aScan;
	}
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
//		/conf.set("hbase.zookeeper.quorum", "hz-hbase3.photo.163.org,hz-hbase4.photo.163.org,hz-hbase5.photo.163.org");
	
		
		Path outputPath = new Path(args[0]);
		String tableName = args[1];
		
		System.out.println(" outputPath:"+outputPath);
		System.out.println("  tableName:"+tableName);
	
	    conf.set("zookeeper.znode.parent", "/hbase-sc");
        conf.set("hbase.zookeeper.quorum", "xxxx,xxx,xxx");
  
        
		Job job = Job.getInstance(conf, "HBaseDemoInputFromHbaseJob"); // 利用job取代了jobclient

		job.setJarByClass(HBaseDemoInputFromHbaseJob.class);
		
		
		//set mapper
		TableMapReduceUtil.initTableMapperJob(tableName, getScan(), HBaseDemoInputFromHbaseJobMapper.class, Text.class, Text.class, job);
	
		//set redurecer
		job.setReducerClass(HBaseDemoInputFromHbaseJobReducer.class);
		
		
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
	
		return job.waitForCompletion(true)?0:1;
	}
	

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new HBaseDemoInputFromHbaseJob(), args);
	}
}
