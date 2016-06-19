package ylj.demo.hbase.java;

import java.io.IOException;


import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.security.UserGroupInformation;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class HPooledTableHelper {

	final static Logger logger = LoggerFactory.getLogger(HPooledTableHelper.class);

	// 声明静态配置
	Configuration hbaseConf = null;
	// HTablePool tablePool=null;
	HConnection connection = null;

	public  void init() throws Exception {

		// 会自动加载hbase-site.xml,在classpath下面
		this.hbaseConf = HBaseConfiguration.create();
		connection = HConnectionManager.createConnection(hbaseConf);

	}

	public  void init(String quorum,String znode) throws Exception {

		// 会自动加载hbase-site.xml,在classpath下面
		this.hbaseConf = HBaseConfiguration.create();
		this.hbaseConf.set("hbase.zookeeper.quorum", quorum);
		this.hbaseConf.set("zookeeper.znode.parent", znode);
	
		connection = HConnectionManager.createConnection(hbaseConf);

	}

	//hz-hbase3.photo.163.org,hz-hbase4.photo.163.org,hz-hbase5.photo.163.org
	public void init(String krb5ConfPath, String keytabPath, String krbUser) throws Exception {

		// kerberos的配置文件的位置，windows下叫krb5.ini，linux下叫krb5.conf
		System.setProperty("java.security.krb5.conf", krb5ConfPath);
		// win下的配置，防止出现winutil.exe的异常，hadoop.home.dir\bin目录下要有编译好的exe
		// 不配这个属性也没影响，就是会有个异常，不影响运行
		// System.setProperty("hadoop.home.dir",
		// "D:\\Hadoop_src\\hadoop-2.2.0");
		// 会自动加载hbase-site.xml,在classpath下面
		hbaseConf = HBaseConfiguration.create();
		// 使用keytab登陆
		UserGroupInformation.setConfiguration(hbaseConf);
		System.setProperty("java.security.krb5.conf", krb5ConfPath);
		UserGroupInformation.loginUserFromKeytab(krbUser, keytabPath);
		// 定时调用更新kerberos（10小时过期），推荐用守护线程定期调用
		UserGroupInformation.getCurrentUser().reloginFromKeytab();

		connection = HConnectionManager.createConnection(hbaseConf);

		//use the connection for other access to the cluster
		
	

	}

	public void close() {
		try {
			connection.close();
		} catch (IOException e) {
			logger.error("close exception .", e);
		}
	}

	

	public void put(String tableName, Put put) throws IOException {

		HTableInterface table =null;
		try {
			table = connection.getTable(tableName);
			table.put(put);

		} finally {
			if (table != null) {
				table.close();
			}

		}

	}
	public HTableInterface getTable(String tableName) throws IOException{
		return connection.getTable(tableName);
	}
	public void puts(String tableName, List<Put> puts,long writeBufferSize) throws IOException {

		HTableInterface table = null;
		try {
			table = connection.getTable(tableName);
			table.setAutoFlush(false, true);
			table.setWriteBufferSize(writeBufferSize);
			
			table.put(puts);
			
			table.flushCommits();
			
		} finally {
			if (table != null) {
				table.close();
			}

		}

	}

	public int delete(String tableName, Delete delete) throws IOException {

		HTableInterface table =null;
		try {
			table = connection.getTable(tableName);
			table.delete(delete);
			return 1;
		} finally {
			if (table != null) {
				table.close();
			}

		}

	}

	public int deletes(String tableName, List<Delete> deletes,long writeBufferSize) throws IOException {

		HTableInterface table =null;
		try {
			int toDeleteSize=deletes.size();
			table = connection.getTable(tableName);
			table.setAutoFlush(false, true);
			table.setWriteBufferSize(writeBufferSize);	
			table.delete(deletes);			
			table.flushCommits();
			return toDeleteSize;
		} finally {
			if (table != null) {
				table.close();
			}

		}

	}
	public void coprocessor(String tableName, Get get) throws IOException {
		HTableInterface table =null;
		try {
			
			table = connection.getTable(tableName);
			
		//	table.coprocessorService(row);
		//	table.coprocessorService(service, startKey, endKey, callable)
		//	table.coprocessorService(service, startKey, endKey, callable, callback);
			
			return ;
		} finally {
			if (table != null) {
				table.close();
			}

		}
	}

	/*
	public void deletesBulk(String tableName,Scan scan){
		
		
	
		  HTableInterface table = null;
			ResultScanner rs = null;
			try {
				table = connection.getTable(tableName);
				 long noOfDeletedRows = 0L;
				 Batch.Call<BulkDeleteProtocol, BulkDeleteResponse> callable = 
				     new Batch.Call<BulkDeleteProtocol, BulkDeleteResponse>() {
				   public BulkDeleteResponse call(BulkDeleteProtocol instance) throws IOException {
				     return instance.deleteRows(scan, BulkDeleteProtocol.DeleteType, timestamp, rowBatchSize);
				   }
				 };
				 Map<byte[], BulkDeleteResponse> result = table.coprocessorExec(BulkDeleteProtocol.class,
				      scan.getStartRow(), scan.getStopRow(), callable);
				  for (BulkDeleteResponse response : result.values()) {
				    noOfDeletedRows = response.getRowsDeleted();
				  }
				  

			} finally {
				if (rs != null) {
					rs.close();
				}
				if (table != null) {
					table.close();
				}
			}
	}
	*/
	public HRecord get(String tableName, Get get) throws IOException {

		HTableInterface table = null;
		try {

			table = connection.getTable(tableName);
			Result result = table.get(get);

			NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilies = result.getNoVersionMap();
			if (columnFamilies == null)
				return null;

			HRecord aRow = new HRecord(result.getRow(),columnFamilies);
			return aRow;
		} finally {
			if (table != null) {
				table.close();
			}

		}

	}

	public HRecord[] gets(String tableName, List<Get> gets) throws IOException {

		HTableInterface table =null;
		try {

			// get.addColumn(family, qualifier)
			// get.addFamily(family)
			table = connection.getTable(tableName);
			Result[] results = table.get(gets);
			HRecord[] hresults = new HRecord[results.length];
			for (int i = 0; i < results.length; i++) {
				NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilies = results[i].getNoVersionMap();
				if (columnFamilies == null)
					return null;

				hresults[i] = new HRecord(results[i].getRow(), columnFamilies);
			}

			return hresults;
		} finally {
			if (table != null) {
				table.close();
			}

		}

	}

	public HRecord[] scan(String tablename, Scan s) throws IOException {

		HTableInterface table = null;
		ResultScanner rs = null;
		try {
			table = connection.getTable(tablename);
			rs = table.getScanner(s);
			
			//table.
			//table.batch(actions)
			List<HRecord> rows = new LinkedList<HRecord>();
			
			//rs.next(nbRows)
			for (Result r : rs) {
				// r.get
				NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilies = r.getNoVersionMap();

				HRecord aRow = new HRecord(r.getRow(),columnFamilies);
				rows.add(aRow);
			}

			return rows.toArray(new HRecord[rows.size()]);

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (table != null) {
				table.close();
			}
		}
	}
	public long rowCount(String tablename) throws Exception{
	
        
        HTableInterface table = null;
		ResultScanner rs = null;
		try {
			table = connection.getTable(tablename);
			Scan scan = new Scan();  	        		       
			scan.setFilter(new FirstKeyOnlyFilter());  
			rs = table.getScanner(scan);
			long rowCount=0;
		
		    for (Result result :  rs) {  
		    	rowCount++;
	        }  
			//table.
			return rowCount;

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (table != null) {
				table.close();
			}
		}
	}
	public long rowCount(String tablename,Scan s) throws Exception{
	
        
        HTableInterface table = null;
		ResultScanner rs = null;
		try {
			table = connection.getTable(tablename);
			        		       
			s.setFilter(new FirstKeyOnlyFilter());  
			rs = table.getScanner(s);
			long rowCount=0;
		
		    for (Result result :  rs) {  
		    	rowCount++;
	        }  
			//table.
			return rowCount;

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (table != null) {
				table.close();
			}
		}
	}
	/*
	public interface CounterProtocol extends CoprocessorProtocol {
	    public long count(byte[] start, byte[] end) throws IOException;
	}
	public class CounterEndPoint extends BaseEndpointCoprocessor implements CounterProtocol {
		 
	    @Override
	    public long count(byte[] start, byte []end) throws IOException {
	        // aggregate at each region
	        Scan scan = new Scan();
	        long numRow = 0;
	 
	        InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion()
	                .getScanner(scan);
	        try {
	            List<KeyValue> curVals = new ArrayList<KeyValue>();
	            boolean hasMore = false;
	            do {
	                curVals.clear();
	                hasMore = scanner.next(curVals);
	                if (Bytes.compareTo(curVals.get(0).getRow(), start)<0) {
	                    continue;
	                }
	                if (Bytes.compareTo(curVals.get(0).getRow(), end)>= 0) {
	                    break;
	                }
	                numRow++;
	            } while (hasMore);
	        } finally {
	            scanner.close();
	        }
	        return numRow;
	    }
	 
	}
	*/
	
	/*
	private Map<String, Map<String, byte[]>> toStrKeyMap(NavigableMap<byte[], NavigableMap<byte[], byte[]>> byteMapMap) {

		Map<String, Map<String, byte[]>> strMapMap = new HashMap<String, Map<String, byte[]>>();
		for (Entry<byte[], NavigableMap<byte[], byte[]>> entry : byteMapMap.entrySet()) {

			NavigableMap<byte[], byte[]> byteMap = entry.getValue();
			Map<String, byte[]> strMap = new HashMap<String, byte[]>();
			for (Entry<byte[], byte[]> entry2 : byteMap.entrySet()) {
				strMap.put(new String(entry2.getKey(), StandardCharsets.UTF_8), entry2.getValue());
			}

			strMapMap.put(new String(entry.getKey(), StandardCharsets.UTF_8), strMap);
		}

		return strMapMap;
	}
	*/
	
}
