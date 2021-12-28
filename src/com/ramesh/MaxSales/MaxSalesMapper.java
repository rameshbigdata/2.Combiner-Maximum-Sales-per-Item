package com.ramesh.MaxSales;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
	public class MaxSalesMapper extends Mapper<Object, Text, Text, IntWritable> {
	@Override
	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
	String[] items;
	items=line.toString().split("\t");
	String itemname=items[0];
	int count=Integer.parseInt(items[1]);
	 context.write(new Text(itemname), new IntWritable(count));
	
	}
	}