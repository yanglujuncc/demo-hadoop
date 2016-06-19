package ylj.demo.hadoop2;


import java.io.IOException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


public class DemoMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	
	
	private Pattern articleFullpattern = Pattern.compile("/nc/article/[a-zA-Z0-9_]*/full.html");
	private Matcher articleFullMatcher = articleFullpattern.matcher("");

	private Pattern articleHeadpattern = Pattern.compile("/nc/article/[a-zA-Z0-9_]*/head.html");
	private Matcher articleHeadMatcher = articleHeadpattern.matcher("");

	private Pattern articleTailpattern = Pattern.compile("/nc/article/[a-zA-Z0-9_]*/tail.html");
	private Matcher articleTailMatcher = articleTailpattern.matcher("");

	private Pattern articleStatpattern = Pattern.compile("/nc/article/[a-zA-Z0-9_]*/stat.html");
	private Matcher articleStatMatcher = articleStatpattern.matcher("");

	private int startIndex = "/nc/article/".length();

	int accept = 0;
	int drop = 0;

	
	@Override
	public void setup(Context context) {

		Counter counter=context.getCounter("info", "mapper setup call");
		counter.increment(1);
	
	}
	@Override
	public void cleanup(Context context) {

		Counter counter=context.getCounter("info", "mapper cleanup call");
		counter.increment(1);
	
	}
		
		
	@Override
	public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
	
	
		String line = value.toString();

	
		context.write(new Text(line), new LongWritable(1));
			

	}

}
