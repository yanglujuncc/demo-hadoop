package ylj.demo.hadoop2;



import java.util.Arrays;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DemoToolRunner2  extends Configured implements Tool{


	public static void usage(){
		System.out.println("Use : inputDir outputDir ");
	}
	
	@Override
	public int run(String[] args) throws Exception {
	
		runAJob(args);
		
		runAJob(args);
		
		return 0;
	}
	
	public int runAJob(String[] args) throws Exception {

		System.out.println("  run args :" + Arrays.toString(args));

		if (args.length != 2) {
			usage();
			return -1;
		}

		String inputPathsStr = args[0];
		String outputPathStr = args[1];

	//	System.out.println(">>inputPathsStr :" + inputPathsStr);
	//	System.out.println(">>outputPathStr :" + outputPathStr);
		// System.out.println(conf.get("mapred.job.tracker"));  
	   //  System.out.println(conf.get("fs.default.name"));  

		Configuration conf = getConf();

		System.out.println("              mapred.map.tasks:" + conf.get("mapredurce.map.tasks"));
		System.out.println("           mapred.reduce.tasks:" + conf.get("mapredurce.reduce.tasks"));

		System.out.println("        mapred.child.java.opts:" + conf.get("mapredurce.child.java.opts"));
		System.out.println("    mapred.map.child.java.opts:" + conf.get("mapredurce.map.child.java.opts"));
		System.out.println(" mapred.reduce.child.java.opts:" + conf.get("mapredurce.reduce.child.java.opts"));
		System.out.println("                       tmpjars:" + conf.get("tmpjars"));

		Job job =  Job.getInstance(conf, "AccountClickSequenceTextNginx");   // 鍒╃敤job鍙栦唬浜唈obclient
		

		job.setJarByClass(DemoToolRunner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
		job.setMapperClass(DemoMapper.class);
		
		job.setCombinerClass(DemoReducer.class);
		job.setCombinerKeyGroupingComparatorClass(DemoGroupingComparator.class);
		//GroupingComparator 
		
		job.setPartitionerClass(DemoPartitioner.class);
		job.setReducerClass(DemoReducer.class);
		
		job.setSortComparatorClass(DemoSortComparator.class);
		job.setGroupingComparatorClass(DemoGroupingComparator.class);
		
		//GroupingComparator  判断是否放在同一个，reduce(Text key2,Iterable<LongWritable> value2s），只有==起作用
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		
		
		String reduceTasksStr=conf.get("mapredurce.reduce.tasks");
		if(reduceTasksStr!=null){
			int reduceTasks=Integer.parseInt(reduceTasksStr);
			
			if(reduceTasks>1){

			}
		}
		
		
		FileInputFormat.setInputPaths(job, inputPathsStr);	
		FileOutputFormat.setOutputPath(job, new Path(outputPathStr));

		boolean failed=false;
	    if (!job.waitForCompletion(true)) {
	    	failed=true;
        }
	    
	    if(failed)
	    	System.exit(1);

		return 0;

	}
	public static void main(String[] args) throws Exception {


		System.out.println("  main args "+Arrays.toString(args));
		int res = ToolRunner.run( new DemoToolRunner2(), args);
	
		System.exit(res);

	}

}
