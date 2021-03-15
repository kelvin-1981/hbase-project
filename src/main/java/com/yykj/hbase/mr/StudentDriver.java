package com.yykj.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StudentDriver extends Configuration implements Tool{

	private Configuration conf = null;
	
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = HBaseConfiguration.create();
		
		int run = ToolRunner.run(configuration, new StudentDriver(), args);
		System.out.println(run);
	}
	
	/**
	 * 
	 */
	public int run(String[] arg0) throws Exception {
		
		Job job = Job.getInstance(conf);
		
		//Driver
		job.setJarByClass(StudentDriver.class);
		
		//Mapper
		TableMapReduceUtil.initTableMapperJob("student",
				new Scan(), 
				StudentMapper.class,
				ImmutableBytesWritable.class, 
				Put.class, 
				job);
		
		//reducer
		TableMapReduceUtil.initTableReducerJob("student_cp", 
				StudentReducer.class, 
				job);
		
		boolean result = job.waitForCompletion(true);
		
		return result ? 0 : 1;
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return this.conf;
	}

	public void setConf(Configuration configuration) {
		// TODO Auto-generated method stub
		this.conf = configuration;
	}
}
