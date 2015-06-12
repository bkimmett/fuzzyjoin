

package ca.uvic.csc.research;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.ID;

//import ca.uvic.csc.research.IntArrayWritable;
import ca.uvic.csc.research.MetadataShortWritable;

import java.lang.Integer;
import java.lang.Long;
import java.lang.String;
//import java.util.HashMap;
import java.util.ArrayList;
//import java.io.File;
//import java.io.PrintWriter;


public class FuzzyJoinSplitBin {
	public static Path in_path;
	public static boolean first_string = true;
	private static int threshold;
	
	private static int string_length; 
	

	//NOT YET FOR USE WITH BINARY STRINGS OF LENGTH > 32
	
  	public static class FuzzyJoinSplitBinMapper 
       extends Mapper<NullWritable, IntWritable, MetadataShortWritable, IntWritable>{
    
    private IntWritable key1;
    private IntWritable[] keyOut;
    private ObjectWritable[] valueOut;
    private MetadataShortWritable valueOut_ready = new MetadataShortWritable();
    private static int[] string_chunks;
	private static int[] shift_positions;
	private static boolean chunks_set = false;
	private static int threshold;
    
      
    public void map(NullWritable key, IntWritable value, Context context
                    ) throws IOException, InterruptedException {
     Configuration conf = context.getConfiguration();
	 
     
     
     
     int value_i = value.get();
      String[] line = value.toString().trim().split("\\s",1);
      //expected: 1 value: the original string
	
	 
     //if(line[0].length() != string_length && string_length != -1){
		//	throw new IOException("Error: The strings are not all the same length. Check for malformed strings and nonwhitespace characters that appear invisible.");
		/*} else */if (!chunks_set) {
			chunks_set = true;
			threshold = conf.getInt("fuzzyJSplit.threshold", 1);
			/*if(line[0].length() > 32){
				throw new IOException("Error: a binary string is too long.");
			} */
			//figure out where splits should occur
			
			string_chunks = new int[threshold+1];
			shift_positions = new int[threshold+1];
			
			for(int i = 0; i<threshold+1; i++){ //retrieve masking array from conf
				string_chunks[i] = conf.getInt("fuzzyJSplit.stringChunk"+i, -1);
				shift_positions[i] = conf.getInt("fuzzyJSplit.shiftPos"+i, -1);
			}
			
		}
		//Text orig_text = new Text(line[0]);
		MetadataShortWritable keyOut = new MetadataShortWritable();
		
		for(int j=0; j<threshold+1; j++){	//divide into d+1 splits
			
			keyOut.setMeta((byte)j);
			keyOut.set((short)((value_i & string_chunks[j]) >> shift_positions[j]));
			
			
			//(j==threshold)?line[0].substring(string_chunks[j]):line[0].substring(string_chunks[j],string_chunks[j+1]));
			//that ternary expression basically means 'go from point 1 to point 2' for most chunks and 'from point n to the end of the string' for the last one.
			context.write(keyOut, value);
		}

     
      }
    }
  
   public static class InARowPartitioner extends Partitioner<MetadataShortWritable, IntWritable> implements Configurable 
	{
	
	private Configuration conf;
  	private int numSplits;

		public void setConf(Configuration conf) {
  		  this.conf = conf;
  		  numSplits = conf.getInt("fuzzyJSplit.threshold", 1)+1;
 		 }
  
 		 public Configuration getConf() {
 		   return conf;
		  }
	
	   @Override
 	   public int getPartition(MetadataShortWritable key, IntWritable value,int numReduceTasks)
 	   {	
 	   	   int segment = (int)key.get();
 	   	   if(segment < 0){
 	   	   	segment *= -1;
 	   	   	segment |= (1 << 15); //recover sign bit
 	   	   }
 	   	   segment *= numSplits;
 	   	   segment += (int)key.getMeta();
 	   	   
 	   	   return segment % numReduceTasks; // simple modulo hashing for relatively equal distribution. 
 	   }  
	}

	public static class FuzzyJoinSplitNullReducer
       extends Reducer<MetadataShortWritable,IntWritable,Text,Text> {
       //returns without reducing anything. Used for measuring communication cost.
		public void reduce(MetadataShortWritable key, Iterable<IntWritable> values, 
		   Context context
		   ) throws IOException, InterruptedException {
	   
		  	 return;
		   }
  
  }
  
  
  public static class FuzzyJoinSplitBinReducer
       extends Reducer<MetadataShortWritable,IntWritable,Text,Text> {
       
       
    private static int[] string_chunks;
	private static int[] shift_positions;
	private static boolean chunks_set = false;
	private static int threshold;
	private static boolean null_absolute;
	private long records_written = 0;
       
    final static int[] byte_ones = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4}; // lookup table for hamming distance between two bytes

	//public static int partial_chunks = 0;
	//public static int total_chunks = 0;

	private boolean substring_verify(int one, int two, int family){ //check that all preceding chunks of the strings are not equal
		for(int i = family-1; i >= 0; i--){ //check i'th substring
			if((one & string_chunks[i]) == (two & string_chunks[i])){ return false;}
		}
		return true;
	}


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
                
                
               /* for(int i=0; i<one.length; i++){
                        byte difference = (byte)(one[i] ^ two[i]); //XOR a pair of single bytes
                        //System.err.println(difference+" "+(difference % 16)+" "+((difference >>> 4) & 0x0F));
                        distance += byte_ones[difference & 0x0F]; //check distance w/ lookup table, first half: ~~~~1111
                        distance += byte_ones[(difference >>> 4) & 0x0F]; //check distance w/ lookup table, second half: 1111~~~~
                }*/
                //System.err.println(one[one.length-1]+" "+two[one.length-1]+" "+distance);
                return distance;
        }

    public void reduce(MetadataShortWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        //System.err.println("Reducer's ready.");
        null_absolute = conf.getBoolean("fuzzyJSplit.null_absolute", false);
        if (!chunks_set) {
			chunks_set = true;
			threshold = conf.getInt("fuzzyJSplit.threshold", 1);
			
			/*if(line[0].length() > 32){
				throw new IOException("Error: a binary string is too long.");
			} */
			//figure out where splits should occur
			
			string_chunks = new int[threshold+1];
			shift_positions = new int[threshold+1];
			
			for(int i = 0; i<threshold+1; i++){ //retrieve masking array from conf
				string_chunks[i] = conf.getInt("fuzzyJSplit.stringChunk"+i, -1);
				shift_positions[i] = conf.getInt("fuzzyJSplit.shiftPos"+i, -1);
			}
			
		}
        
     	ArrayList<Integer> int_list = new ArrayList<Integer>();
     	//System.err.println("reducer: ready to add!");
     	context.setStatus("reduce: reading data");
     	// System.err.println("Key: "+key);
     	for (IntWritable val : values) { // no need to keep a dict; the system tosses by key!!
     		//note: later duplicate records will overwrite earlier ones.
     		int_list.add(val.get());
     		//System.err.println("Value: "+val.get());

       	}
      
      //that's all data from the reducer read in. Next is processing it.
      	
      	
     	 int list_size = int_list.size();
     	 
     	context.setStatus("reduce: joining data");
		for(int i=0; i< list_size; i++){
			if(!null_absolute || i % 10000 == 0){
				context.setStatus("reduce: joining data ("+(i+1)+"/"+list_size+")");
			}
			for(int j=i+1; j< list_size; j++){
				if(compareDistance(int_list.get(i),int_list.get(j)) <= threshold && substring_verify(int_list.get(i),int_list.get(j),(int)key.getMeta())){ // if the distance checks out and the strings are lexicographically first...
					if(!null_absolute){
						context.write(new Text(String.format("%32s", Integer.toBinaryString(int_list.get(i))).replace(" ", "0")), new Text(String.format("%32s", Integer.toBinaryString(int_list.get(j))).replace(" ", "0")));
						//context.write(new Text(int_list.get(i)), new Text(int_list.get(j))); // perform comparison. If a success, kick out the pair
					} else {
						records_written++;
					}
				}
			/*current_chunk++;
			partial_chunks++;
			context.setStatus("reduce: processing chunk "+current_chunk+"/"+list_size+" ("+partial_chunks+"/"+total_chunks+" total)");*/
			}
		}
		
      }
   }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length <5) {
      System.err.println("Usage: FuzzyJoinSplitBin <in> <out> <comparison_threshold> <number_of_reducers> <universe_size>;  Setting <out> to 'null' discards output; 'null-cost' discards after copying to reducer.");
      System.exit(2);
    }
    //conf.setBoolean("mapred.compress.map.output", true);
    //conf.setBoolean("mapred.output.compress", true);
    //conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
    in_path = new Path(otherArgs[0]);

    //conf.setInt("fuzzyJSplit.universe", threshold);
    
    threshold = Integer.parseInt(otherArgs[2]);
    
    string_length = Integer.parseInt(otherArgs[4]);
    conf.setInt("fuzzyJSplit.universe", string_length);
   
    
    //figure out where splits should occur
			
	//string_chunks = new int[threshold+1];
	//shift_positions = new int[threshold+1];
	
	int string_chunk_len = string_length / (threshold+1);
	int string_chunk_remain = string_length % (threshold+1); // this is how many bits are 'leftover' from an equal division and must be added to the front
	int lesser_mask = (1 << string_chunk_len) - 1; //equal to 2^n -1
	int greater_mask = lesser_mask << 1 | 1; //shift one place to the right, add an extra bit
	
	
	int places_left_to_shift = string_length;
	
	
	for(int i = 0; i<threshold+1; i++){
		places_left_to_shift -= (string_chunk_len + ((i<string_chunk_remain)?1:0));
		conf.setInt("fuzzyJSplit.stringChunk"+i, ((i<string_chunk_remain)?greater_mask:lesser_mask) << places_left_to_shift);
		
		//string_chunks[i] = ((i<string_chunk_remain)?greater_mask:lesser_mask) << places_left_to_shift;
		conf.setInt("fuzzyJSplit.shiftPos"+i, places_left_to_shift);
		//shift_positions[i] = places_left_to_shift;
	}
			
    
    
    conf.setInt("fuzzyJSplit.threshold", threshold);
  
  	if(otherArgs[1].equals("null-absolute")){
  		conf.setBoolean("fuzzyJSplit.null_absolute", true);
    }
  
    Job job = new Job(conf, "Fuzzy Join [Splitting Alg., Binary Version: threshold "+threshold+", "+string_length+"-bit universe]");
	job.setNumReduceTasks(Integer.parseInt(otherArgs[3])); 
	//job.setNumReduceTasks(0); 
	job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
    job.setJarByClass(FuzzyJoinSplitBin.class);
    job.setMapperClass(FuzzyJoinSplitBinMapper.class);
    job.setPartitionerClass(InARowPartitioner.class);
    //reducer set below
    job.setMapOutputKeyClass(MetadataShortWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
   	FileInputFormat.addInputPath(job, in_path);
   	
   	if(otherArgs[1].equals("null")){
		System.err.println("Note: ALL OUTPUT DISCARDED");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinSplitBinReducer.class);
	} else if (otherArgs[1].equals("null-cost")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (measuring communication cost)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinSplitNullReducer.class);
	} else if (otherArgs[1].equals("null-absolute")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (completely)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinSplitBinReducer.class);
		//conf.setBoolean("fuzzyJSplit.null_absolute", true); MOVED upwards because of when Job is generated
	} else {
		job.setReducerClass(FuzzyJoinSplitBinReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	}
    
  
    boolean success = job.waitForCompletion(true);
    if(!success){ System.exit(1); }
    /*TaskCompletionEvent[] outputs = job.getTaskCompletionEvents(0);
    File outlog = new File("logs/"+job.getJobID()+"_times");
    outlog.createNewFile();
    PrintWriter outline = new PrintWriter(outlog);
    
    int completionOffset = 0;
    do{
    for( TaskCompletionEvent task: outputs){
    	outline.println(task.getTaskAttemptId()+"\t"+task.getTaskStatus()+"\t"+((float)task.getTaskRunTime()/1000.0));
    }
    	completionOffset += outputs.length;
    	outputs = job.getTaskCompletionEvents(completionOffset);
    } while(outputs.length > 0);
    outline.println(completionOffset);
    outline.close();*/
    
    System.exit(0);
  }
}
