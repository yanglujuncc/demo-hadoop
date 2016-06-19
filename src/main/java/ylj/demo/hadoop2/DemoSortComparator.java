package ylj.demo.hadoop2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DemoSortComparator extends WritableComparator{
	
	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {

		Text p1 = (Text) o1;
		Text p2 = (Text) o2;

	 return p1.toString().compareTo(p2.toString());
 }
}
