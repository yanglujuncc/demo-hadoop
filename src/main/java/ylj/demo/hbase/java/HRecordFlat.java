package ylj.demo.hbase.java;


import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import com.google.common.primitives.UnsignedBytes;

public class HRecordFlat {
	
	public byte[] rowKey;
	public String[] cellKey;  //Family.Qualifier
	public byte[][] cellData;
	
	public HRecordFlat(){
		
	}
	public HRecordFlat(byte[] rowKey,String[] cellKey,byte[][] cellData){
		
		this.rowKey=rowKey;
		this.cellKey=cellKey;
		this.cellData=cellData;
	}
	
	public static class Cell implements Comparable<Cell>{
		String cellKey;
		byte[] cellData;
		public Cell(String cellKey,byte[] cellData){
			this.cellKey=cellKey;
			this.cellData=cellData;
		}
		/* (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(Cell other) {
			
			return cellKey.compareTo(other.cellKey);
		}
	}
		
	private static HRecordFlat fromByteMap(NavigableMap<byte[], NavigableMap<byte[], byte[]>> byteMapMap) throws Exception{
			
		
			List<Cell> cellList=new LinkedList<Cell>();
			
			for (Entry<byte[], NavigableMap<byte[], byte[]>> entry :byteMapMap.entrySet()) {
				
				String family=new String(entry.getKey(),"utf-8");
			
				for(Entry<byte[], byte[]> entry2 :entry.getValue().entrySet()){
					String qualifier=new String(entry.getKey(),"utf-8");
					String cellKey=family+"."+qualifier;
					cellList.add(new Cell( cellKey,entry2.getValue()));
				}			
				
			}
			Collections.sort(cellList);
			
			HRecordFlat newHRecordImmutable=new HRecordFlat();
			newHRecordImmutable.cellKey=new String[cellList.size()];
			newHRecordImmutable.cellData=new byte[cellList.size()][];
			int i=0;
			for(Cell cell:cellList){
				newHRecordImmutable.cellKey[i]=cell.cellKey;
				newHRecordImmutable.cellData[i]=cell.cellData;
				i++;
			}
			
			return newHRecordImmutable;
			
		}
		
	public static HRecordFlat fromHRecord(HRecord hRecord) throws Exception{
		
		HRecordFlat hRecordFlat=fromByteMap(hRecord.familyQualifierMap);
		hRecordFlat.rowKey=hRecord.rowKey;
		
		return hRecordFlat;
		
	}
	
	public HRecord toHRecord(HRecordFlat hRecordFlat) throws Exception{
		
		HRecord aHRecord=new HRecord();
		aHRecord.rowKey=hRecordFlat.rowKey;
		aHRecord.familyQualifierMap=new TreeMap<byte[],NavigableMap<byte[], byte[]>>(UnsignedBytes.lexicographicalComparator());
		
		for(int i=0;i<hRecordFlat.cellKey.length;i++){
			String[] familyQualifier=hRecordFlat.cellKey[i].split("\\.");
			
			byte[] family=familyQualifier[0].getBytes("utf-8");
			byte[] qualifier=familyQualifier[1].getBytes("utf-8");
			byte[] cellData=hRecordFlat.cellData[i];
			
			NavigableMap<byte[], byte[]>qualifierMap=aHRecord.familyQualifierMap.get(family);
			if(qualifierMap==null){
				qualifierMap=new  TreeMap<byte[], byte[]>(UnsignedBytes.lexicographicalComparator());
				aHRecord.familyQualifierMap.put(family, qualifierMap);
			}
			qualifierMap.put(qualifier, cellData);
			
		}
		
		return aHRecord;
	}
	
}
