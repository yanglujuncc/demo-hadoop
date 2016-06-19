/**
 *  @author hzyanglujun
 *  @version  创建时间:2016年3月1日 上午11:53:02
 */
package ylj.demo.hbase.java;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;

/**
 * @author hzyanglujun
 *
 */
public class HCommonCopocessors {

	public static void main(String[] args){
		String coprocessClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
		 
		AggregationClient ac = new AggregationClient(HBaseConfiguration.create());
		
		//ac.
		
	//	ac.rowCount(table, ci, scan)
	//	ac.avg(table, ci, scan)
	}
}
