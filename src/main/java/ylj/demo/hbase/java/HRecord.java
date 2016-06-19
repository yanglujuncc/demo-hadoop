package ylj.demo.hbase.java;


import java.util.NavigableMap;
public class HRecord {
	
	public byte[] rowKey;
	public NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierMap;
	
	public HRecord(){
		
	}
	public HRecord(byte[] rowKey,NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierMap){
		this.rowKey=rowKey;
		this.familyQualifierMap=familyQualifierMap;
	}
}
