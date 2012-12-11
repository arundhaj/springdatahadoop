package com.arundhaj.controller;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value="/hello")
public class HelloController implements BeanFactoryAware {

	Configuration hadoopConfiguration;
	Configuration hbaseConfiguration;
	HbaseTemplate myHbaseTemplate;
	
	@Override
	public void setBeanFactory(BeanFactory context) throws BeansException {
		hadoopConfiguration = (Configuration) context.getBean("hadoopConfiguration");
		hbaseConfiguration = (Configuration) context.getBean("hbaseConfiguration");
		myHbaseTemplate = (HbaseTemplate) context.getBean("myHbaseTemplate");
	}

	@RequestMapping(value="/mapreduce")
	public @ResponseBody String mapreduce() {
		boolean success = false;
		try {
			Path inPath = new Path("/hadoop_ws/wordcount/input");
			Path outPath = new Path("/hadoop_ws/wordcount/output");
			
			Job wcJob = new Job(hadoopConfiguration);
			wcJob.setJarByClass(WordCount.class);
			
			wcJob.setJobName("Wordcount Job");
			
			wcJob.setOutputKeyClass(Text.class);
			wcJob.setOutputValueClass(IntWritable.class);
			
			wcJob.setMapperClass(WordCount.TokenizerMapper.class);
			wcJob.setReducerClass(WordCount.IntSumReducer.class);
			
			wcJob.setInputFormatClass(TextInputFormat.class);
			wcJob.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(wcJob, inPath);
			FileOutputFormat.setOutputPath(wcJob, outPath);
			
			success = wcJob.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return "fs.default.name: " + hadoopConfiguration.get("fs.default.name") + " job status: " + success;
	}

	@RequestMapping(value="/hbase")
	public @ResponseBody String hbase() {
		
		List<String> rows = myHbaseTemplate.find("test", "cf", new RowMapper<String>() {

			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				return Bytes.toString(result.getRow());
			}
			
		});

		String configTest = "hbase.rootdir: " + hbaseConfiguration.get("hbase.rootdir"); 
		
		return "resp size: " + rows.size() + " name: " + rows.toString(); 
	}

}
