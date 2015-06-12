

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
import java.lang.Long;
import java.lang.String;
import java.lang.Math;
//import java.util.HashMap;
import java.util.ArrayList;
//import java.io.File;
//import java.io.PrintWriter;
//import java.math.BigInteger;


public class FuzzyJoinBall2Bin {
	public static Path in_path;
	public static int string_length;
	public static int threshold;

  	public static class FuzzyJoinBall2BinMapper 
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


	private int[] deform(int original, int threshold){
		if(threshold < 1){ System.err.println("WARN: Threshold appears empty."); return new int[0]; } //empty 
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
		 threshold = conf.getInt("fuzzyJBall2Bin.threshold", 1); 
		 string_length = conf.getInt("fuzzyJBall2.universe", 32);
	 
		 context.write(value, value); // the original key
		for(int transform : deform(value.get(), (threshold+1)/2)){// (threshold+1)/2 is basically ceil(threshold/2) for integers because integer division truncates [same as using floor()].
			context.write(new IntWritable(transform), value);
		}
    }
}
  
 public static class StringToIntPartitioner extends Partitioner<IntWritable, IntWritable>
	{
	   @Override
 	   public int getPartition(IntWritable key, IntWritable value,int numReduceTasks)
 	   {	
 	   	   return (int)(Math.abs((long)key.get()) % numReduceTasks); // simple modulo hashing for relatively equal distribution. 
 	   }  
	}
	
	  public static class FuzzyJoinBall2NullReducer
       extends Reducer<IntWritable,IntWritable,Text,Text> {
       //dummy - used for measuring comm. cost
       public void reduce(IntWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
                       return;
                       
        }

       }
  
  
  public static class FuzzyJoinBall2BinReducer
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
	
	final static int[] byte_ones = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4}; // lookup table for hamming distance between two bytes

	//public static int partial_chunks = 0;
	//public static int total_chunks = 0;

	public static int compareDistance(int one, int two){
                //compares the 'string distance' of two binary strings represented as byte arrays. the arrays must be the same size.
                int distance = 0;
                int difference = one ^ two;
                
                while(difference != 0){ // because the shift fills the most significant byte with 0s
                	distance += byte_ones[difference & 0x0F]; //check distance w/ lookup table, first half of least significant byte: ~~~~1111
                	difference >>>= 4;
                	distance += byte_ones[difference & 0x0F]; //  second half: 1111~~~~
                	difference >>>= 4; 
                }
                return distance;
    }
    
    
	
	private int lex(int s, int t, int distance, int threshold){
		//given two binary strings S and T, and a distance between them, this method returns the 'lexicographically first' [read: smallest binary number] string that lies at most threshold/2 single bit-flips between BOTH of them. 
		//'distance' is the actual distance between s and t. 'threshold' is the max number of bit-flips needed to change s into t. It's assumed that s and t already fulfill this property with the set threshold and distance values.
		//it does it like this:
		//scan S and T from left to right [MSB to LSB].
		//if s[i] is a 0, copy it straight.
		//if s[i] is a 1 and t[i] is 0, copy down a 0 as long as we haven't made our maximum of ceil(threshold/2) 1-to-0 swaps.
		//if s[i] and t[i] are both 1's, there is an additional constraint of ceil((threshold-distance)/2) "dual-1" swaps needed to copy down a 0.
		//if s[i] is a 1 and you can't do one of the above two scenarios, copy down a 1.
		//return the altered string.
		//char[] final_chars = new char[s.length()];
		int s_distance = 0;
		int t_distance = distance;
		int final_int = 0;
		
		int total_swaps = (threshold+1)/2; //the +1 is for ceiling()
		int dual_one_swaps = (threshold-distance)/2; //no +1 here. This uses floor().
		for(int i=31; i>= 0; i--){ //move from MSB to LSB
			if(total_swaps > 0 && ((s & (1 << i)) != 0) && (dual_one_swaps > 0 || ((t & (1 << i)) == 0))){ //we can theoretically swap (1>?<0) or (1>?<1)
				///final_chars[i] = '0';
				///DO NOTHING. the bit starts 0 already.
				
				total_swaps--;
				s_distance += 1;
				
				if((t & (1 << i)) != 0){ // (1>0<1) if s[i] and t[i] are both 1 we only have 
					dual_one_swaps--;
					t_distance += 1;
				} else {  //(1>0<0)
					t_distance -= 1;
				}
			} else { //straight bit copy from S
				final_int |= (s & (1 << i));
			}
		}
		//this section checks for an edge case that may occur if not enough swaps have been consumed
		//due to their being very many 1s in T and very many 0s in S.
		if(t_distance > (threshold+1)/2 && total_swaps > 0){
			for(int i=0; i<= 31 && total_swaps > 0 && t_distance > (threshold+1)/2; i++){ // move from LSB to MSB
				if(((s & (1 << i)) == 0) && ((t & (1 << i)) != 0)){ //1 in T, 0 in S (0>1<1)
					final_int |= (1 << i); //change a 0 into a 1
					s_distance += 1;
					t_distance -= 1;
					total_swaps--;
					continue; //we don't need to check the next one
				}
				if(((s & (1 << i)) != 0) && ((t & (1 << i)) != 0) && ((final_int & (1 << i)) == 0)){ //this was a dual-swapped bit (1>0<1), recover it back to 1 (1>1<1)
					final_int |= (1 << i); //change a 0 into a 1 
					s_distance -= 1;
					t_distance -= 1;
					total_swaps++;
				}
			}
		}
		
		return final_int;
	}
	

    public void reduce(IntWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        boolean self_found = false; // am I active?

        Configuration conf = context.getConfiguration();
		distance_threshold = conf.getInt("fuzzyJBall2Bin.threshold", 1); 
		null_absolute = conf.getBoolean("fuzzyJBall2.null_absolute", false);  

     	ArrayList<Integer> int_list = new ArrayList<Integer>();
     	context.setStatus("reduce: reading data");
     	for (IntWritable val : values) { // no need to keep a dict; the system tosses by key!!
     		//note: later duplicate records will overwrite earlier ones.
			if (!self_found && val.equals(key)){ // have I found my own value? theoretically we can have each reducer self-add its value
				self_found = true;
			} 
			int_list.add(val.get());
       	}
      
      //that's all data from the reducer read in. Next is processing it.
      	if(!self_found){ System.err.println("WARN: Reducer has not received its own key: "+key); }
      	
      	int list_size = int_list.size();
      	context.setStatus("reduce: joining data"); // join every item with every other item
      	for(int i=0; i<list_size; i++){
      		if(!null_absolute || i % 10000 == 0){
      			context.setStatus("reduce: joining data ("+(i+1)+"/"+list_size+")");
      		}
      		for(int j=i+1; j<list_size; j++){
      			//boolean test_case = false;
      			//if(int_list.get(i) == 0 && int_list.get(j) == 24){ System.err.println("test case detected - this reducer is "+key.get()); test_case = true;}
      			int distance = compareDistance(int_list.get(i),int_list.get(j));
      			if(distance > distance_threshold){ 
      			//if(test_case){ System.err.println("Failed. Distance too large."); }
      			 continue;}
      			//String lexed = lex(int_list.get(i),int_list.get(j), distance, distance_threshold);
      			//System.err.println("INFO: Pair "+int_list.get(i)+" and "+int_list.get(j)+" has lex "+lexed+", with key "+key+".");
      			//if(test_case){ System.err.println("Lex: "+lex(int_list.get(i),int_list.get(j), distance, distance_threshold)+" with 0, 24, "+distance+", "+distance_threshold+"."); }
      			
      			if(lex(int_list.get(i),int_list.get(j), distance, distance_threshold) == key.get()){
      					//if(test_case){ System.err.println("DONE!");}
					if(!null_absolute){
						context.write(new Text(String.format("%32s", Integer.toBinaryString(int_list.get(i))).replace(" ", "0")), new Text(String.format("%32s", Integer.toBinaryString(int_list.get(j))).replace(" ", "0"))); // have we got a 'lexicographically first' pair? If so, kick out the pair.
					} else {
						records_written++;
					}
				}
      		}
      	}
      }
   }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 5) {
      System.err.println("Usage: FuzzyJoinBall2Bin <in> <out> <comparison_threshold> <number_of_reducers> <universe_size>; Setting <out> to 'null' discards output; 'null-cost' discards after copying to reducer.");
      System.exit(2);
    }
    //conf.setBoolean("mapred.compress.map.output", true);
    //conf.setBoolean("mapred.output.compress", true);
    //conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
    in_path = new Path(otherArgs[0]);
    
    threshold = Integer.parseInt(otherArgs[2]);
    conf.setInt("fuzzyJBall2Bin.threshold", threshold);
  
	string_length = Integer.parseInt(otherArgs[4]); // set custom length threshold
	conf.setInt("fuzzyJBall2.universe", string_length);

    if(otherArgs[1].equals("null-absolute")){
  		conf.setBoolean("fuzzyJBall2.null_absolute", true);
    }
  
    Job job = new Job(conf, "Fuzzy Join [Binary Strings, Ball-Hashing alg. 2: threshold "+threshold+", "+string_length+"-bit universe]");
	
	job.setNumReduceTasks(Integer.parseInt(otherArgs[3])); 
	//job.setNumReduceTasks(0); 
	job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
    job.setJarByClass(FuzzyJoinBall2Bin.class);
    job.setMapperClass(FuzzyJoinBall2BinMapper.class);
    job.setPartitionerClass(StringToIntPartitioner.class);
    //reducer set below
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
   	FileInputFormat.addInputPath(job, in_path);
   	if(otherArgs[1].equals("null")){
		System.err.println("Note: ALL OUTPUT DISCARDED");
		job.setReducerClass(FuzzyJoinBall2BinReducer.class);
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
	} else if (otherArgs[1].equals("null-cost")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (measuring communication cost)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinBall2NullReducer.class);
	} else if (otherArgs[1].equals("null-absolute")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (completely)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinBall2BinReducer.class);
	} else {
		job.setReducerClass(FuzzyJoinBall2BinReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	}
    
  
    boolean success = job.waitForCompletion(true);
    if(!success){ System.exit(1); }
    System.exit(0);
  }
}
