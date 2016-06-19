package ylj.demo.hbase.java;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class HAdminHelper {

	final static Logger logger = LoggerFactory.getLogger(HAdminHelper.class);

	// 声明静态配置
	Configuration hbaseConf = null;
	// HTablePool tablePool=null;
	HConnection connection = null;

	public void init() throws Exception {


		// 会自动加载hbase-site.xml,在classpath下面
		this.hbaseConf = HBaseConfiguration.create();
		UserGroupInformation.setConfiguration(hbaseConf);	
		connection = HConnectionManager.createConnection(hbaseConf);
		
	}
	public void init(String krb5ConfPath,String keytabPath,String krbUser) throws Exception {

		// kerberos的配置文件的位置，windows下叫krb5.ini，linux下叫krb5.conf
		System.setProperty("java.security.krb5.conf",krb5ConfPath);
		// win下的配置，防止出现winutil.exe的异常，hadoop.home.dir\bin目录下要有编译好的exe
		// 不配这个属性也没影响，就是会有个异常，不影响运行
		// System.setProperty("hadoop.home.dir",
		// "D:\\Hadoop_src\\hadoop-2.2.0");
		// 会自动加载hbase-site.xml,在classpath下面
		hbaseConf = HBaseConfiguration.create();
		// 使用keytab登陆
		UserGroupInformation.setConfiguration(hbaseConf);
		UserGroupInformation.loginUserFromKeytab(krbUser, keytabPath);
		// 定时调用更新kerberos（10小时过期），推荐用守护线程定期调用
		UserGroupInformation.getCurrentUser().reloginFromKeytab();

		connection = HConnectionManager.createConnection(hbaseConf);

		// use the connection for other access to the cluster

	}

	public void close() {
		try {
			connection.close();
		} catch (IOException e) {	
			logger.error("close exception .", e);
		}
	}

	/*
	 * 创建表
	 * 
	 * @tableName 表名
	 * 
	 * @family 列族列表
	 */
	public void listTable() throws Exception {

		HBaseAdmin admin = new HBaseAdmin(hbaseConf);

		for (HTableDescriptor hTableDescriptor : admin.listTables()) {
			logger.info("" + hTableDescriptor);
		}
		admin.close();

	}

	public void descTable(String tableName) throws Exception {

		HTableInterface table = connection.getTable(tableName);
		HTableDescriptor desc = table.getTableDescriptor();

		logger.info(desc.toString());

		table.close();
	}

	public void creatTable(String tableName, String[] family) throws Exception {

		HBaseAdmin admin = new HBaseAdmin(hbaseConf);

		TableName name = TableName.valueOf(tableName);

		HTableDescriptor desc = new HTableDescriptor(name);
		
		//desc.set
	//	desc.
		for (int i = 0; i < family.length; i++) {
			desc.addFamily(new HColumnDescriptor(family[i]));
		}
		if (admin.tableExists(tableName)) {
			logger.info("table Exists!");

		} else {
			admin.createTable(desc);

			// admin.addColumn(tableName, column);
			logger.info("create table Success!");
		}
		admin.close();

	}

	public void addTableColumnFamily(String tableName, String[] family) throws Exception {

		HBaseAdmin admin = new HBaseAdmin(hbaseConf);

		for (int i = 0; i < family.length; i++) {
			HColumnDescriptor columnDesc=new HColumnDescriptor(family[i]);
			int timeToLive=3600*24*30; //a month 
			columnDesc.setTimeToLive(timeToLive);
			//columnDesc.setBlocksize(s);
			
			//setBloomFilterType(NONE | ROW | ROWCOL
			//
			columnDesc.setBloomFilterType(BloomType.ROW);
			columnDesc.setMaxVersions(1);
			columnDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
		
			admin.addColumn(tableName, new HColumnDescriptor(columnDesc));
		}

		admin.close();

	}

	public void deleteTableColumnFamily(String tableName, String[] family) throws Exception {

		HBaseAdmin admin = new HBaseAdmin(hbaseConf);

		for (int i = 0; i < family.length; i++) {
			admin.deleteColumn(tableName, family[i]);
		}

		admin.close();

	}

	public void deleteTable(String tableName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(hbaseConf);

		admin.deleteTable(tableName);
	
		
		logger.info("delete table Success!  table:" + tableName);
		admin.close();
	}
	public void deleteAllRecords(String tableName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(hbaseConf);
		
		logger.info("isTableEnabled - "+tableName+",  Enabled:" + admin.isTableEnabled(tableName));
		
		admin.disableTable(tableName);
		logger.info("after disableTable , isTableEnabled - "+tableName+",  Enabled:" + admin.isTableEnabled(tableName));
		admin.truncateTable(TableName.valueOf(tableName), false);
		logger.info("after truncateTable , isTableEnabled - "+tableName+",  Enabled:" + admin.isTableEnabled(tableName));
	//	admin.enableTable(tableName);
		logger.info("delete all table records Success!  table:" + tableName);
		admin.close();
		
		
		

	}

	/*
	public void putRow(String tableName, byte[] rowKey, Map<byte[], Map<byte[], byte[]>> columnFamilies) throws IOException {

		HTableInterface table = connection.getTable(tableName);
		try {

			Put put = new Put(rowKey);
			for (Entry<byte[], Map<byte[], byte[]>> entry : columnFamilies.entrySet()) {
				for (Entry<byte[], byte[]> entry2 : entry.getValue().entrySet()) {
					put.add(entry.getKey(), entry2.getKey(), entry2.getValue());
					
				}
			}
		
			table.put(put);
	
			table.flushCommits();

		} finally {
			if (table != null) {
				table.close();
			}

		}

	}
	*/
	
	

	

	public static void main(String[] args) throws Exception {
		
		 DOMConfigurator.configureAndWatch("conf/log4j.xml");
		 
		HAdminHelper aNetEaseHZHBaseHelper = new HAdminHelper();
		
		
		// String krb5ConfPath="D:/workspace/adflow/conf/hbase/krb5.conf";
		// String keytabPath="D:/workspace/adflow/conf/hbase/ad.keytab";
		// String user="ad/dev@HADOOP.HZ.NETEASE.COM";
			
		//aNetEaseHZHBaseHelper.init(krb5ConfPath,keytabPath,user);

		
		aNetEaseHZHBaseHelper.init();
		
		
		
		String tableName="news_app_doc_day_snapshot";
	
		
		aNetEaseHZHBaseHelper.descTable(tableName);
		//aNetEaseHZHBaseHelper.listTable();
		//aNetEaseHZHBaseHelper.descTable(tableName);
		
	// 删记录
	//	aNetEaseHZHBaseHelper.deleteAllRecords(AppHBaseADDeliverProgressAdopter_1_0.HtableNameOfADProgress);
	//	aNetEaseHZHBaseHelper.deleteAllRecords(WebHBaseADDeliverProgressAdopter_1_0.HtableNameOfADProgress);
		
	// 加字段
	//	String[] family=new String[]{WebHBaseADDeliverProgressAdopter_1_0.ColumFamilyName};
	//  aNetEaseHZHBaseHelper.addTableColumnFamily(WebHBaseADDeliverProgressAdopter_1_0.HtableNameOfADProgress, family);
	//  aNetEaseHZHBaseHelper.addTableColumnFamily(AppHBaseADDeliverProgressAdopter_1_0.HtableNameOfADProgress, family);
		
	//  删字段	
	//  String[] family=new String[]{"cf"};
	//	aNetEaseHZHBaseHelper.deleteTableColumnFamily(AppHBaseADDeliverProgressAdopter_1_0.HtableNameOfADProgress, family);
	//	aNetEaseHZHBaseHelper.deleteTableColumnFamily(WebHBaseADDeliverProgressAdopter_1_0.HtableNameOfADProgress, family);
	
		
	//	aNetEaseHZHBaseHelper.descTable(AppHBaseADDeliverProgressAdopter_1_0.HtableNameOfADProgress);
	//	aNetEaseHZHBaseHelper.descTable(WebHBaseADDeliverProgressAdopter_1_0.HtableNameOfADProgress);
	
		aNetEaseHZHBaseHelper.close();
		
	}
}
