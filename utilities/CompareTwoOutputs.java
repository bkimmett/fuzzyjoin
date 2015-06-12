

package ca.uvic.csc.research;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.ID;

import ca.uvic.csc.research.IntArrayWritable;
//import ca.uvic.csc.research.ObjectArrayWritable;

import java.lang.Integer;
import java.lang.Long;
import java.lang.String;
//import java.util.HashMap;
import java.util.ArrayList;
//import java.io.File;
//import java.io.PrintWriter;


public class CompareTwoOutputs {
	public static Path in_path_1;
	public static Path in_path_2;


	//NOT YET FOR USE WITH BINARY STRINGS OF LENGTH > 32
	//with 132 reducers the highest granularity we can do is 15, using 120 reducers
	//
	
  	public static class CompareTwoOutputsMapper 
       extends Mapper<Object, Text, IntArrayWritable, IntWritable>{
  
    private IntWritable key1;
    private IntWritable key2;
    private IntWritable[] keyOut;
    
   
    
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     Configuration conf = context.getConfiguration();
     
      String path_one = conf.get("compareOutputs.pathOne");
      if(path_one == null){ throw new IOException("Path 1 is missing."); }
      String path_two = conf.get("compareOutputs.pathTwo");
     if(path_two == null){ throw new IOException("Path 2 is missing."); }
     
     FileSplit file = (FileSplit)context.getInputSplit();
      Path currentPath = file.getPath();
     
     
     
      String[] line = value.toString().trim().split("\\s",2);

// simple version: 
//R: a, b, val => b, 1, a, val
//S: b, c, val => b, 2, c, val
      
      key1 = new IntWritable(Integer.parseInt(line[0], 2));
      key2 = new IntWritable(Integer.parseInt(line[1], 2));
      
      if(key1.get() > key2.get()){
      	int temp = key2.get();
      	key2.set(key1.get());
      	key1.set(temp);
      }
       keyOut = new IntWritable[2];
       keyOut[0] = key1;
       keyOut[1] = key2;
		
		if(currentPath.getParent().toString().endsWith(path_one)){
       context.write(new IntArrayWritable(keyOut), new IntWritable(1));
      
      } else if(currentPath.getParent().toString().endsWith(path_two)) {
      
      		context.write(new IntArrayWritable(keyOut), new IntWritable(2));
      } else {
      	System.err.println("ERROR: Unidentified input path: "+currentPath.getParent());
      	System.err.println("vs [1] "+in_path_1);
      	System.err.println("vs [2] "+in_path_2);
      }
      
     

    }
  }
  
  
   public static class GuaranteedHashPartitioner extends Partitioner<IntArrayWritable, IntWritable>
	{
	   @Override
 	   public int getPartition(IntArrayWritable key, IntWritable value,int numReduceTasks)
 	   {	
 	   	   return ((Math.abs(key.get(0).get()) % numReduceTasks) + (Math.abs(key.get(1).get()) % numReduceTasks)) % numReduceTasks; // dual modulo then addition to avoid heavy reliance on one single bucket.
 	   }  
	}
  
  
  
  public static class CompareTwoOutputsReducer
       extends Reducer<IntArrayWritable,IntWritable,Text,Text> {


    public void reduce(IntArrayWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {              
     	int value_1_found = 0;
     	int value_2_found = 0;
     	//System.err.println("reducer: ready to add!");
     	context.setStatus("reduce: reading data");
     	for (IntWritable val : values) { // no need to keep a dict; the system tosses by key!!
     		//note: later duplicate records will overwrite earlier ones.
     		if(val.get() == 1){
     			value_1_found++;
     		}
     		if(val.get() == 2){
     			value_2_found++;
     		}
     		
       	}
       	
    	if(value_1_found > 0 && value_2_found == 0){
    		context.write(new Text("Error: In first input but not in second:"), new Text(String.format("%32s", Integer.toBinaryString(key.get(0).get())).replace(" ", "0")+"/"+String.format("%32s", Integer.toBinaryString(key.get(1).get())).replace(" ", "0")));
    	
    	} 
    	
    	if(value_1_found == 0 && value_2_found > 0){
    		context.write(new Text("Error: In second input but not in first:"), new Text(String.format("%32s", Integer.toBinaryString(key.get(0).get())).replace(" ", "0")+"/"+String.format("%32s", Integer.toBinaryString(key.get(1).get())).replace(" ", "0")));
    	} 
    	if(value_1_found > 1){
    		context.write(new Text("Warning: Appears "+value_1_found+" times in first input:"), new Text(String.format("%32s", Integer.toBinaryString(key.get(0).get())).replace(" ", "0")+"/"+String.format("%32s", Integer.toBinaryString(key.get(1).get())).replace(" ", "0")));
    	} 
    	if(value_2_found > 1){
    		context.write(new Text("Warning: Appears "+value_2_found+" times in first input:"), new Text(String.format("%32s", Integer.toBinaryString(key.get(0).get())).replace(" ", "0")+"/"+String.format("%32s", Integer.toBinaryString(key.get(1).get())).replace(" ", "0")));
    	} 
    
      }
   }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length !=4) {
      System.err.println("Usage: CompareTwoOutputs <in1> <in2> <out> <number_of_reducers> Setting <out> to 'null' discards output.");
      System.exit(2);
    }
    //conf.setBoolean("mapred.compress.map.output", true);
    //conf.setBoolean("mapred.output.compress", true);
    //conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
   	in_path_1 = new Path(otherArgs[0]);
    in_path_2 = new Path(otherArgs[1]);
    System.err.println("Path one: "+in_path_1);
    System.err.println("Path two: "+in_path_2);
	conf.set("compareOutputs.pathOne", in_path_1.toString());
   	conf.set("compareOutputs.pathTwo", in_path_2.toString());
   	
  
    Job job = new Job(conf, "Output Comparison");
	job.setNumReduceTasks(Integer.parseInt(otherArgs[3])); 
	//job.setNumReduceTasks(0); 
    job.setJarByClass(CompareTwoOutputs.class);
    job.setMapperClass(CompareTwoOutputsMapper.class);
    job.setPartitionerClass(GuaranteedHashPartitioner.class);
    job.setReducerClass(CompareTwoOutputsReducer.class);
    job.setMapOutputKeyClass(IntArrayWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
   	FileInputFormat.addInputPath(job, in_path_1);
   	FileInputFormat.addInputPath(job, in_path_2);
   
   	if(conf.get("compareOutputs.pathOne") == null){
   		System.err.println("Path one didn't store. Exiting...");
   		System.exit(1);
   	}
   	if(conf.get("compareOutputs.pathTwo") == null){
   		System.err.println("Path two didn't store. Exiting...");
   		System.exit(1);
   	}
   	
   	
   	if(otherArgs[2].equals("null")){
    	System.err.println("Note: ALL OUTPUT DISCARDED");
    	job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
    } else {
    	FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    }
    
  
    boolean success = job.waitForCompletion(true);
    if(!success){ System.exit(1); }

    
    System.exit(0);
  }
}
