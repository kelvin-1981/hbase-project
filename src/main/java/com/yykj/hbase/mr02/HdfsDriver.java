package com.yykj.hbase.mr02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HdfsDriver extends Configuration implements Tool{

	private Configuration conf = null;
	
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = HBaseConfiguration.create();
		
		int run = ToolRunner.run(configuration, new HdfsDriver(), args);
		
		System.out.println(run);
	}

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf;
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return this.conf;
	}

	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(HdfsDriver.class);
		
		job.setMapperClass(HdfsMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		TableMapReduceUtil.initTableReducerJob("people", HdfsReducer.class, job);
		
		FileInputFormat.setInputPaths(job, args[0]);
		
		boolean result = job.waitForCompletion(true);
		return result ? 0 : 1;
	}

	
}
