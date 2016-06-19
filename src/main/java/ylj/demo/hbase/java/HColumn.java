/**
 *  @author hzyanglujun
 *  @version  创建时间:2016年2月26日 下午5:55:35
 */
package ylj.demo.hbase.java;

/**
 * @author hzyanglujun
 *
 */
public class HColumn {

	public byte[] family;
	public byte[] qualifier;

	public HColumn(byte[] family, byte[] qualifier) {
		this.family = family;
		this.qualifier = qualifier;
	}

}
