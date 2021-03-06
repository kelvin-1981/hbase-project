package com.yykj.hbase.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class TestMain {
	
	private static Admin admin = null;
	
	private static Connection conn = null;
	
	private static Configuration conf = null;
	
	static{
		
		conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://node21:9000/hbase");
		conf.set("hbase.zookeeper.quorum", "node21,node22,node23");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		try {
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin(); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param args
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
      
		getData("student", "1001");
		//scanData("student");
		
		//deleteCellData("student", "1001", "info", "sex");
		//deleteRowData("student", "1001");
		
//		int code = 1000;
//		for(int i = 1; i <= 1000; i++){
//			code += i;
//			putData("student", String.valueOf(code), "info", "name", "kelvin");
//			putData("student", String.valueOf(code), "info", "sex", "male");
//			putData("student", String.valueOf(code), "info", "age", "18");
//		}
		
//		putData("student", "1001", "info", "name", "kelvin");
//		putData("student", "1001", "info", "sex", "male");
//		putData("student", "1001", "info", "age", "18");
		
//		deleteTable("student");
		
//		createTable("student", "info");
		
//		boolean result = isTableExists("sales");
//      System.out.println(result);
	}
	
	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @throws IOException 
	 */
	private static void getData(String tableName,String rowKey) throws IOException{
		
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		Get get = new Get(Bytes.toBytes(rowKey));
		//??????CF???CN??????
		//get.addColumn(family, qualifier);
		//????????????????????????
		//get.setMaxVersions(maxVersions);
		
		Result result = table.get(get);
		Cell[] rawCells = result.rawCells();
		
		for (Cell cell : rawCells) {
			System.out.println(" RowKey=" + Bytes.toString(CellUtil.cloneRow(cell)) + 
					" CF=" + Bytes.toString(CellUtil.cloneFamily(cell)) + 
					" CN=" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
					" VALUE=" + Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}
	
	/**
	 * ??????????????????
	 * @param tableName
	 * @param startRow
	 * @param stopRow
	 * @throws IOException
	 */
	private static void scanData(String tableName) throws IOException{
		
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		int index = 1;
		Scan scan = new Scan();
		//scan.setStartRow(Bytes.toBytes(startRow));
		//scan.setStopRow(Bytes.toBytes(stopRow));
		//??????????????? ????????????
		//scan.setCaching(caching);
		
		ResultScanner scanner = table.getScanner(scan);
	
		
		
		for (Result result : scanner) {
			index += 1;
			Cell[] rawCells = result.rawCells();
			for (Cell cell : rawCells) {
				System.out.println("index=" + index + " RowKey=" + Bytes.toString(CellUtil.cloneRow(cell)) + 
						" CF=" + Bytes.toString(CellUtil.cloneFamily(cell)) + 
						" CN=" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
						" VALUE=" + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
	}
	
	
	/**
	 * ????????????
	 * @param tableName
	 * @param rowKey
	 * @param cf
	 * @param cn
	 * @throws IOException 
	 */
	@SuppressWarnings("unused")
	private static void deleteRowData(String tableName,String rowKey) throws IOException{
		
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		
		table.delete(delete);
		table.close();
		
		System.out.println("????????????????????????");
	}
	
	/**
	 * ????????????
	 * @param tableName
	 * @param rowKey
	 * @param cf
	 * @param cn
	 * @throws IOException 
	 */
	private static void deleteCellData(String tableName,String rowKey,String cf,String cn) throws IOException{
		
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
		
		table.delete(delete);
		table.close();
		
		System.out.println("????????????????????????");
	}
	
	/**
	 * ??????/????????????
	 * @param tableName
	 * @param rowKey
	 * @param cf
	 * @param column
	 * @param value
	 * @return
	 * @throws IOException 
	 */
	@SuppressWarnings("unused")
	private static void putData(String tableName, String rowKey, String cf, String col,String value) throws IOException{
		
		Table table = conn.getTable(TableName.valueOf(tableName)); 
				
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
		
		table.put(put);
		
		System.out.println("?????????????????????");
	} 
	
	/**
	 * ???????????????
	 * @param tableName
	 * @throws IOException 
	 */
	@SuppressWarnings("unused")
	private static void deleteTable(String tableName) throws IOException{
		
		if(!isTableExists(tableName)){
			System.out.println("???????????????????????????");
		}
		
		if(!admin.isTableDisabled(TableName.valueOf(tableName))){
			admin.disableTables(tableName);
		}
		
		admin.deleteTable(TableName.valueOf(tableName));
		
		System.out.println("????????????????????????");
	}
	
	/**
	 * ???????????????
	 * @param tableName
	 * @param cfs????????? ????????????
	 * @return
	 * @throws IOException 
	 */
	@SuppressWarnings("unused")
	private static void createTable(String tableName,String... cfs) throws IOException {
		
		if(isTableExists(tableName)){
			System.out.println("???????????????????????????");
			return;
		}
		
		//??????????????????
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		
		//????????????
		for (String cf : cfs) {
			//??????????????????
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
			//?????????????????????
			//hColumnDescriptor.setMaxVersions(3);
			//?????????
			desc.addFamily(hColumnDescriptor);
		}
		
		admin.createTable(desc);
		
		System.out.println("????????????????????????");
	}

	/**
	 * ?????????????????????
	 * @param tableName
	 * @return
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	private static boolean isTableExists(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		if(admin == null){
			return false;
		}
		
		 boolean result = admin.tableExists(TableName.valueOf(tableName));
		 admin.close();
		 return result;
	}	
}
