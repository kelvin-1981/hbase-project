package com.yykj.hbase.mr02;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HdfsMapper extends Mapper<LongWritable, Text, NullWritable, Put> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(" ");
		
		Put put = new Put(Bytes.toBytes(fields[0]));
		for (String info : fields) {
			put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
			put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("sex"), Bytes.toBytes(fields[2]));
			put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"), Bytes.toBytes(fields[3]));
		}
		
		context.write(NullWritable.get(), put);
	}

	
}
