package com.yykj.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

public class StudentMapper extends TableMapper<ImmutableBytesWritable, Put> {

	/**
	 * key:RowKey
	 * value:RowKey 数据行数据
	 */
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Context context)
			throws IOException, InterruptedException {
		
		Put put = new Put(key.get());
		
		Cell[] rawCells = value.rawCells();
		for (Cell cell : rawCells) {
			//只选择name数据
			if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
				put.add(cell);
			}
		}
		
		context.write(key, put);
	}

	
}
