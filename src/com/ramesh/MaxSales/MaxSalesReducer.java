package com.ramesh.MaxSales;

 
import java.io.IOException; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
	
	public class MaxSalesReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
	    private IntWritable result = new IntWritable();
		public  void reduce(Text itemname,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
		{
			int sum=0;
			int maxSalesValue = Integer.MIN_VALUE;
		    for(IntWritable val : values) 
		    {
		        maxSalesValue = Math.max(maxSalesValue, val.get());
		    }  
		      result.set(maxSalesValue);
		      context.write(itemname, result);
 
		}

	}
