

package ca.uvic.csc.research;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.ID;

//import ca.uvic.csc.research.IntArrayWritable;
//import ca.uvic.csc.research.ObjectArrayWritable;

import java.lang.Integer;
//import java.lang.Long;
import java.lang.String;
import java.lang.Math;
//import java.util.HashMap;
import java.util.ArrayList;
//import java.io.File;
//import java.io.PrintWriter;
//import java.math.BigInteger;


public class FuzzyJoinBall3Bin {
	public static Path in_path;
	public static int string_length; // size of an int
	public static int threshold;
	
  	public static class FuzzyJoinBall3BinMapper 
       extends Mapper<NullWritable, IntWritable, IntWritable, IntWritable>{
	
	private int binomial(int top, int bottom){
		//System.err.println("BINOMIAL: Calculating "+top+" choose "+bottom+".");
		//calculates a binomial coefficient (top choose bottom).
		if(top-bottom < bottom){ bottom = top-bottom; } // efficiency; cut down on extra multiplications. 'bottom' is always smaller, 'top-bottom' always larger.
		long top_total = 1; //top factorial divided by larger bottom factorial
		long bottom_total = 1; //smaller bottom factorial
		for(int firstfact = top; firstfact > top-bottom; firstfact--){
			//calculates the upper factorial divided by the larger of the lower factorials 
			//so, for (a+b)!/(a!b!) it calculates (a+b)!/a! assuming a is larger. It does this by
			//(a+b)*(a+b-1)*...*(a+1).
			top_total *= firstfact;
		}
		for(int secondfact = 2; secondfact <= bottom; secondfact++){
			//this is just a straight 1-to-n factorial. The 1 step has been left out.
			bottom_total *= secondfact;
		}
		//System.err.println("BINOMIAL: Part-factors are "+top_total+" and "+bottom_total+", yielding "+(top_total / bottom_total)+".");
		return (int)(top_total / bottom_total);
	}
	
	private int binaryStringCompare(int one, int two){
		//Returns the comparison of numbers in the usual way, adapting for the sign bit.
		//IF BOTH POSITIVE: normal compare. A > B means A-B = > 0 and vice versa.
		//IF BOTH NEGATIVE: 255-254 [A > B]. A-B = -1 - -2 = 1 so >0. It still works. Normal compare.
		
		
		//IF A NEGATIVE, B POSITIVE: [A > B]. Return a positive number. 
		if(one < 0 && two >= 0){return 1;}
		
		//IF A POSITIVE, B NEGATIVE: [A < B]. Return a negative number. 
		if(one >= 0 && two < 0){return -1;}
		
		//returns less than, equal to, or greater than 0
		//if [one] is <, =, or > [two], respectively.

		return one-two;
	}


	private int[] deform(int original, int threshold){
		if(threshold < 1){ return new int[0]; } //empty 
		if(string_length < threshold){ threshold = string_length; }
		int expected_output_size = 0;
		for (int choose=1; choose <= threshold; choose++){
			expected_output_size += binomial(string_length, choose);
		}
		//System.err.println("Deform activate, length "+string_length+", threshold "+threshold+". Expected # of transformations: "+expected_output_size);
		int[] output = new int[expected_output_size];
		int current_output = 0;
		int current_mask;
		for (int choose=1; choose <= threshold; choose++){
			if(choose <= 30){ 
				current_mask = (1 << choose)-1;
				
			} else { // choose = 31
				current_mask = 2147483647; // work around the sign bit
			}
		
			int[] deformation_pointers = new int[choose]; // resize the array as needed to hold a pointer for each deformation location
				for(int i=0; i<choose; i++){ deformation_pointers[i] = i; } // fill with starting points
  Def_Loop: while(true){
				for(int shift=0; shift+(deformation_pointers[choose-1])<string_length; shift++){ // make this deform and all shifted versions thereof
					//System.err.println("deform mask: "+Integer.toBinaryString(current_mask));
					output[current_output] = original ^ (current_mask << shift);
					current_output++;
				}
				int current_pointer = choose-1;
				int pointer_offset = 0;
				if(current_pointer == 0){ break; } // we're not moving the pointer if we have only one
				while(true){ //move pointers until we find a new combination that works
					current_mask &= ~(1 << deformation_pointers[current_pointer]); //unset bit about to be moved
					deformation_pointers[current_pointer]++;
					if (deformation_pointers[current_pointer] + pointer_offset >= string_length){ //pointer fell off the end, or fell somewhere there'd be no room for the pointers after them
						current_pointer--;
						pointer_offset++;
						if(current_pointer < 1){ // never move the first pointer, it's shifted
							break Def_Loop; //all pointers have fallen off the end, let's get out of here!
						}
					} else { //cascade pointers after the successfully moved pointer
						int successfully_placed_value = deformation_pointers[current_pointer]; //so if a pointer is placed at '2', the next ones get '3', '4', '5' etc
						current_mask |= (1 << deformation_pointers[current_pointer]); //set successfully moved bit
						int original_pointer_offset = pointer_offset;
						while(pointer_offset > 0){
							current_pointer++;
							pointer_offset--;
							deformation_pointers[current_pointer] = successfully_placed_value + (original_pointer_offset - pointer_offset);
							current_mask |= (1 << deformation_pointers[current_pointer]);
						}
						break; //stop moving pointers, let's get to the next deformation
					}
				} //'move pointers' while ends
			} // def_loop while ends
		} //for loop ends	
		return output;
	}

    public void map(NullWritable key, IntWritable value, Context context
                    ) throws IOException, InterruptedException {
     Configuration conf = context.getConfiguration();
		 threshold = conf.getInt("fuzzyJBall3.threshold", 1); 
		 string_length = conf.getInt("fuzzyJBall3.universe", 32);

		context.write(value, value); // the original key
		for(int transform : deform(value.get(), threshold)){
			if(binaryStringCompare(transform,value.get()) < 0){ // check for duplicates IN MAPPER! #improvement
			context.write(new IntWritable(transform), value);
			}
		}
    }
}
  
  public static class StringToIntPartitioner extends Partitioner<IntWritable, IntWritable>
	{
	   @Override
 	   public int getPartition(IntWritable key, IntWritable value,int numReduceTasks)
 	   {	
 	   	   return (int)(Math.abs((long)key.get()) % numReduceTasks); // simple modulo hashing for relatively equal distribution. now accepts negative numbers.
 	   }  
	}
  
  public static class FuzzyJoinBall3NullReducer
       extends Reducer<IntWritable,IntWritable,Text,Text> {
       //dummy - used for measuring comm. cost
       public void reduce(IntWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
                       return;
                       
        }

       }
  
  
  public static class FuzzyJoinBall3BinReducer
       extends Reducer<IntWritable,IntWritable,Text,Text> {

	private int distance_threshold; //how close two strings have to be to qualify. Higher = more permissive
	//public static int partial_chunks = 0;
	//public static int total_chunks = 0;
	private static boolean null_absolute;
	private long records_written = 0;

	private int binaryStringCompare(int one, int two){
		//Returns the comparison of numbers in the usual way, adapting for the sign bit.
		//IF BOTH POSITIVE: normal compare. A > B means A-B = > 0 and vice versa.
		//IF BOTH NEGATIVE: 255-254 [A > B]. A-B = -1 - -2 = 1 so >0. It still works. Normal compare.
		
		
		//IF A NEGATIVE, B POSITIVE: [A > B]. Return a positive number. 
		if(one < 0 && two >= 0){return 1;}
		
		//IF A POSITIVE, B NEGATIVE: [A < B]. Return a negative number. 
		if(one >= 0 && two < 0){return -1;}
		
		//returns less than, equal to, or greater than 0
		//if [one] is <, =, or > [two], respectively.

		return one-two;
	}

    public void reduce(IntWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        boolean self_found = false; // am I active?
        Integer self = null; // 
        
        
        Configuration conf = context.getConfiguration();
		distance_threshold = conf.getInt("fuzzyJBall3.threshold", 1);      
		null_absolute = conf.getBoolean("fuzzyJBall3.null_absolute", false);           
        
     	ArrayList<Integer> int_list = new ArrayList<Integer>();
     	//System.err.println("reducer: ready to add!");
     	context.setStatus("reduce: reading data");
     	for (IntWritable val : values) { // no need to keep a dict; the system tosses by key!!
     		//note: later duplicate records will overwrite earlier ones.
			if (!self_found && val.equals(key)){
				self_found = true;
				self = key.get();
			} else { // this assume duplicate 'self' values will be joined as normal.
				//if
				int_list.add(val.get());
			}
       	}
      
      //that's all data from the reducer read in. Next is processing it.
      	if(!self_found){ return; } //though, if we haven't received the nod, give up.
      	Text self_string = new Text(String.format("%32s", Integer.toBinaryString(self)).replace(" ", "0"));
      	
      	context.setStatus("reduce: joining data");
     	for(int val: int_list){
     		if(!null_absolute){
				context.write(self_string, new Text(String.format("%32s", Integer.toBinaryString(val)).replace(" ", "0")));
			} else {
				records_written++;
			}
     	} 
      }
   }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 5) {
      System.err.println("Usage: FuzzyJoinBall3Bin <in> <out> <comparison_threshold> <number_of_reducers> <universe_size>; Setting <out> to 'null' discards output; 'null-cost' discards after copying to reducer.");
      System.exit(2);
    }
    //conf.setBoolean("mapred.compress.map.output", true);
    //conf.setBoolean("mapred.output.compress", true);
    //conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
    in_path = new Path(otherArgs[0]);
    
    threshold = Integer.parseInt(otherArgs[2]);
    conf.setInt("fuzzyJBall3.threshold", threshold);
    
	string_length = Integer.parseInt(otherArgs[4]); // set custom length threshold
	conf.setInt("fuzzyJBall3.universe", string_length);
	
	if(otherArgs[1].equals("null-absolute")){
  		conf.setBoolean("fuzzyJBall3.null_absolute", true);
    }
    
  
    Job job = new Job(conf, "Fuzzy Join [Binary Strings, Ball-Hashing alg. 3: threshold "+threshold+", "+string_length+"-bit universe]");
	
	job.setNumReduceTasks(Integer.parseInt(otherArgs[3])); 
	//job.setNumReduceTasks(0); 
	job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
    job.setJarByClass(FuzzyJoinBall3Bin.class);
    job.setMapperClass(FuzzyJoinBall3BinMapper.class);
    job.setPartitionerClass(StringToIntPartitioner.class);
	//reducer set below
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
   	FileInputFormat.addInputPath(job, in_path);
   	if(otherArgs[1].equals("null")){
		System.err.println("Note: ALL OUTPUT DISCARDED");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinBall3BinReducer.class);
	} else if (otherArgs[1].equals("null-cost")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (measuring communication cost)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinBall3NullReducer.class);
	} else if (otherArgs[1].equals("null-absolute")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (completely)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinBall3BinReducer.class);
	} else {
		job.setReducerClass(FuzzyJoinBall3BinReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	}
    
  
    boolean success = job.waitForCompletion(true);
    if(!success){ System.exit(1); }
    System.exit(0);
  }
}
