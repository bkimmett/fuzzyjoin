

package ca.uvic.csc.research;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
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
import ca.uvic.csc.research.MetadataIntWritable;
//import ca.uvic.csc.research.ObjectArrayWritable;

import java.lang.Integer;
//import java.lang.Long;
import java.lang.String;
import java.util.Arrays;
import java.lang.StrictMath; //for extra precision on Ceil()'s output. behaves like Math.
//import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
//import java.io.File;
//import java.io.PrintWriter;
//import java.StrictMath.BigInteger;
import java.util.Random;


public class FuzzyJoinAnchorNewBin {
	public static Path in_path;
	public static int string_length; // size of an int
	public static int threshold;
	public static HashSet<Integer> pointsbox;
	public static boolean reverse_mode;
	public static long numPoints = -1;
		
	final static int[] byte_ones = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4}; //lookup table for hamming distance
	
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
     
     protected static int binaryStringCompare(int one, int two){
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
	

	protected static int binomial(int top, int bottom){
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


	public static class FuzzyJoinAnchorNewBinMapper
	   extends Mapper<NullWritable, IntWritable, IntWritable, MetadataIntWritable>{
	
	private static int[] deform(int original, int threshold){ //new version, outputs in lex. order
		if(threshold < 1){ return new int[0]; } //empty
		if(string_length < threshold){ threshold = string_length; }
		int expected_output_size = 0;
		int[] translation_array = new int[string_length];
		int array_front = 0;
		int array_rear = string_length-1;
		//this array of translation bits goes from LSB to MSB. Technically the exact order is [most significant 1] > [least significant 1] > [least significant 0] > [most significant 0] as the pointers start at the rear of this array and move towards the front. Thus, LSBs and 1s are deformed more often.
		/*if(original < 0 && string_length == 32){ //sign bit is 1
			translation_array[array_front] = 31; 
			array_front++;
		} else if(string_length == 32) { //sign bit is 0
			translation_array[array_rear] = 31; 
			array_rear--;
		}*/
		
		for(int i = string_length-1; i >= 0; i--){ //move from MSB to LSB of the string
			if((original & (1 << i)) != 0){ //bit set
				translation_array[array_front] = i; 
				array_front++;			
			} else { //bit unset
				translation_array[array_rear] = i;
				array_rear--;
			}
		}
		//System.err.println("Translation array for "+original+"["+String.format("%8s", Integer.toBinaryString(original)).replace(" ", "0")+"]: "+Arrays.toString(translation_array));
		//the point of the above is to get the system to prioritize turning 1 bits to 0 from MSB to LSB, then making 0s 1 from LSB to MSB. In other words, lexicographically first strings are generated first.
		
		for (int choose=1; choose <= threshold; choose++){
			expected_output_size += binomial(string_length, choose);
		}
		int[] output = new int[expected_output_size];
		int current_output = 0;
		int current_mask;
		for (int choose=1; choose <= threshold; choose++){
			current_mask = 0; 
			for(int bit=0; bit<choose; bit++){ // this is the new mask generation routine, which because the mask is deformed must fill the mask bit by bit. Sorry!
				current_mask |= (1 << translation_array[bit]);
			}
			
		
			int[] deformation_pointers = new int[string_length-choose]; //pointers reference zeros in this version
				for(int i=1; i<=string_length-choose; i++){ deformation_pointers[i-1] = string_length-i; } // fill with starting points
				//System.out.println("pointers placed: "+Arrays.toString(deformation_pointers));
  Def_Loop: while(true){
  				output[current_output] = original ^ current_mask;
				current_output++;
  

				int current_pointer = (string_length-choose)-1;
				int pointer_offset = 0;
				if(current_pointer < 0){ break;} //if string_length == choose, no pointers can be moved
				
				while(true){ //move pointers until we find a new combination that works
					current_mask |= (1 << translation_array[deformation_pointers[current_pointer]]); //unset bit about to be moved
					deformation_pointers[current_pointer]--;
					if (deformation_pointers[current_pointer] - pointer_offset < 0){ //pointer fell off the end, or fell somewhere there'd be no room for the pointers after them
						current_pointer--;
						pointer_offset++;
						if(current_pointer < 0){ // out of pointers!
							break Def_Loop; //all pointers have fallen off the end, let's get out of here!
						}
					} else { //cascade pointers after the successfully moved pointer
						int successfully_placed_value = deformation_pointers[current_pointer]; //so if a pointer is placed at '5', the next ones get '4', '3', '2' etc
						current_mask &= ~(1 << translation_array[deformation_pointers[current_pointer]]); //set successfully moved bit
						int original_pointer_offset = pointer_offset;
						while(pointer_offset > 0){
							current_pointer++;
							pointer_offset--;
							deformation_pointers[current_pointer] = successfully_placed_value - (original_pointer_offset - pointer_offset);
							current_mask &= ~(1 << translation_array[deformation_pointers[current_pointer]]);
						}
						//System.out.println("pointers reshuffled: "+Arrays.toString(deformation_pointers));
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
		threshold = conf.getInt("fuzzyJAnchor.threshold", 1);
		string_length = conf.getInt("fuzzyJAnchor.universe", 32);
		if(numPoints == -1){
			///unpack points array. trigger only if values empty
			reverse_mode =	 conf.getBoolean("fuzzyJAnchor.reverseAnchorPoints", false);
		
			numPoints = conf.getLong("fuzzyJAnchor.numAnchorPoints", 0);
			pointsbox = new HashSet<Integer>((int)(numPoints+numPoints/3+1));
			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Reader reader = new SequenceFile.Reader( fs, new Path("/temp/aptable.apt"), conf);
			NullWritable k = NullWritable.get();
			IntWritable v = new IntWritable();
			for(long i = 0; i< numPoints; i++){
				reader.next(k, v);
				pointsbox.add(v.get());
			}
			reader.close();
		}
		

		if(conf.getBoolean("fuzzyJAnchor.deform_mode", true)){ //are we using deformation mode?
		//YES

			//context.write(value, value); // the original key
			boolean made_closest = false;
			int target_threshold = ((string_length+1)/2 < threshold) ? string_length : 2*threshold;
			//if the distance threshold is > half the string length, cap the number of deforms 
			for(int transform : deform(value.get(), target_threshold)){
				if(pointsbox.contains(transform) ^ reverse_mode){ // must be in anchor points list.
					if(made_closest){
						context.write(new IntWritable(transform), new MetadataIntWritable(value.get()));
						//System.err.println(value.get()+" ["+String.format("%8s", Integer.toBinaryString(value.get())).replace(" ", "0")+"] sent to anchor point "+transform+" ["+String.format("%8s", Integer.toBinaryString(transform)).replace(" ", "0")+"]");
					} else {
						context.write(new IntWritable(transform), new MetadataIntWritable(value.get(), (byte)0x01));
						made_closest = true; //home reducer found!
						//System.err.println(value.get()+" ["+String.format("%8s", Integer.toBinaryString(value.get())).replace(" ", "0")+"] sent to anchor point "+transform+" ["+String.format("%8s", Integer.toBinaryString(transform)).replace(" ", "0")+"] < HOME");
					}
				}
			}
			if(!made_closest){
				System.err.println("ERROR!! A string reached no anchor points. This map-reduce job has failed and should be re-run.");
				throw new IOException("String "+value.get()+" ("+String.format("%32s", Integer.toBinaryString(value.get())).replace(" ", "0")+") reached no anchor points.");
			}
		} else { //NO, we're using linear scan mode
			int closest_point = value.get();
			int closest_distance = -1;
			int dist;
			for(int point: pointsbox){
				dist = compareDistance(value.get(),point);
				if(dist > threshold*2){continue;}
				if(dist == 0){ continue;}
				
				if(closest_distance == -1){ //first point
					closest_distance = dist;
					closest_point = point;
				} else if(dist < closest_distance){
					context.write(new IntWritable(closest_point), new MetadataIntWritable(value.get(), (byte)0)); // the old one isn't the closest reducer
					closest_point = point;
					closest_distance = dist;
				} else if (dist > closest_distance){
					context.write(new IntWritable(point), new MetadataIntWritable(value.get(), (byte)0)); //not home reducer
				} else { //dist == closest_distance
					if(binaryStringCompare(point, closest_point) < 0){ //new point is lesser and thus lexfirst
						context.write(new IntWritable(closest_point), new MetadataIntWritable(value.get(), (byte)0));
						closest_point = point;
						closest_distance = dist;
					} else { //older point is still lexfirst
						context.write(new IntWritable(point), new MetadataIntWritable(value.get(), (byte)0));
					}
				}
		
			}
			if(closest_distance == -1){
				System.err.println("ERROR!! A string reached no anchor points. This map-reduce job has failed and should be re-run.");
				throw new IOException("String "+value.get()+" ("+String.format("%32s", Integer.toBinaryString(value.get())).replace(" ", "0")+") reached no anchor points.");
			} else {
				context.write(new IntWritable(closest_point), new MetadataIntWritable(value.get(), (byte)0x01)); //output last point [home]
			}
		}	
	}
}

  public static class StringToIntPartitioner extends Partitioner<IntWritable, MetadataIntWritable>
	{
	   @Override
	   public int getPartition(IntWritable key, MetadataIntWritable value,int numReduceTasks)
	   {	
		   return (int)(StrictMath.abs((long)key.get()) % numReduceTasks); // simple modulo hashing for relatively equal distribution.
	   }
	}


	public static class FuzzyJoinAnchorNullReducer
	   extends Reducer<IntWritable,MetadataIntWritable,Text,Text> {
	   //returns without reducing anything. Used for measuring communication cost.
	   	public void reduce(IntWritable key, Iterable<MetadataIntWritable> values,
					   Context context
					   ) throws IOException, InterruptedException {
	   		return;
	   }
	   
	   }

  public static class FuzzyJoinAnchorNewBinReducer
	   extends Reducer<IntWritable,MetadataIntWritable,Text,Text> {

	private int distance_threshold; //how close two strings have to be to qualify. Higher = more permissive
	//public static int partial_chunks = 0;
	//public static int total_chunks = 0;
	private static boolean null_absolute;
	private long records_written = 0;


	

	public void reduce(IntWritable key, Iterable<MetadataIntWritable> values,
					   Context context
					   ) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		distance_threshold = conf.getInt("fuzzyJAnchor.threshold", 1);
		null_absolute = conf.getBoolean("fuzzyJAnchor.null_absolute", false);  				
		
		ArrayList<Integer> int_list = new ArrayList<Integer>();
		ArrayList<Integer> int_home_list = new ArrayList<Integer>();
		
		int_list.add(key.get()); //add 'self' to reducer as non-home int
		//System.err.println("reducer: ready to add!");
		context.setStatus("reduce: reading data");
		for (MetadataIntWritable val : values) { // no need to keep a dict; the system tosses by key!!
			//note: later duplicate records will overwrite earlier ones.
			if (val.getMeta() == (byte)0x01){ // home reducer
				int_home_list.add(val.get());
			} else { // not
				int_list.add(val.get());
			}
		}
	
	  //that's all data from the reducer read in. Next is processing it.
		
		int main_list_size = int_list.size();
     	int home_list_size = int_home_list.size();
     	
     	/*if(key.get() == 1){
     		System.err.println("home array: "+int_home_list);
     		System.err.println("the rest array: "+int_list);
     	}*/
		
		//context.setStatus("reduce: joining data [local pairs]");
		for(int i=0; i< home_list_size; i++){
			context.setStatus("reduce: joining data [local pairs] ("+(i+1)+"/"+home_list_size+")");
			for(int j=i+1; j< home_list_size; j++){
				if(compareDistance(int_home_list.get(i),int_home_list.get(j)) <= distance_threshold){
					if(!null_absolute){
						context.write(new Text(String.format("%32s", Integer.toBinaryString(int_home_list.get(i))).replace(" ", "0")), new Text(String.format("%32s", Integer.toBinaryString(int_home_list.get(j))).replace(" ", "0"))); // perform comparison. If a success, kick out the pair
					} else {
						records_written++;
					}
				}
			}
		}
		for(int i=0; i< home_list_size; i++){
			context.setStatus("reduce: joining data [extended pairs] ("+(i+1)+"/"+home_list_size+")");
			for(int j=0; j< main_list_size; j++){
				if(binaryStringCompare(int_home_list.get(i),int_list.get(j)) < 0 && compareDistance(int_home_list.get(i),int_list.get(j)) <= distance_threshold){
					if(!null_absolute){
						context.write(new Text(String.format("%32s", Integer.toBinaryString(int_home_list.get(i))).replace(" ", "0")), new Text(String.format("%32s", Integer.toBinaryString(int_list.get(j))).replace(" ", "0"))); // perform comparison. If a success, kick out the pair
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
	  System.err.println("Usage: FuzzyJoinAnchorNewBin <in> <out> <comparison_threshold> <number_of_reducers> <universe_size>; Setting <out> to 'null' discards output; 'null-cost' discards after copying to reducer.");
	  System.exit(2);
	}
	//conf.setBoolean("mapred.compress.map.output", true);
	//conf.setBoolean("mapred.output.compress", true);
	//conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
	in_path = new Path(otherArgs[0]);
	
	threshold = Integer.parseInt(otherArgs[2]);
	conf.setInt("fuzzyJAnchor.threshold", threshold);
	
	string_length = Integer.parseInt(otherArgs[4]); // set custom length threshold
	conf.setInt("fuzzyJAnchor.universe", string_length);

	if(otherArgs[1].equals("null-absolute")){
  		conf.setBoolean("fuzzyJAnchor.null_absolute", true);
    }

	//generate anchor points table here!

	boolean reverse_mode = false;
	
	long universe_size = 1l << string_length; //256 for 8 bits
	double log_fraction = StrictMath.log(universe_size*1000); //12.452 for 8 bits
	int choose_fraction = 0;
	for (int choose=1; choose <= threshold; choose++){
		choose_fraction += binomial(string_length, choose); //8 for 8 bits, t1
	}
	
	long target_points = (long)StrictMath.ceil(log_fraction*((double)universe_size/(double)choose_fraction));
	long choose_fraction_2 = choose_fraction;
	for (int choose=threshold+1; choose <= (((threshold*2) <=  string_length)?threshold*2:string_length); choose++){
		choose_fraction_2 += binomial(string_length, choose);
	}
	
	
	if(target_points < choose_fraction_2){ //which mode is less costly to do? target_points represents O(|A|). choose_fraction_2 represents O(B(2d)).
		conf.setBoolean("fuzzyJAnchor.deform_mode",false); //linear scan [point check]. |A| < B(2d).
		System.err.println("Using linear scan mode."); 	
	} else {
		conf.setBoolean("fuzzyJAnchor.deform_mode",true); //query expansion [deformations]. |A| > B(2d).
		System.err.println("Using deformation (query expansion) mode.");
	}
	
	if (target_points > universe_size){ //
		System.err.println("WARNING: predicted number of points is greater than universe size.");
		target_points = 0;
		reverse_mode = true;
	} else if (target_points > universe_size / 2){ //if the set is larger than 50% of the universe, pick the points NOT in the set!!
		target_points = universe_size - target_points;
		reverse_mode = true;
	}
	
	if(reverse_mode){
		System.err.println("#Anchor Points Used: "+(universe_size-target_points)+"/"+universe_size+" ("+((double)(universe_size-target_points)/(double)universe_size)*100.0+"%) [reverse mode - "+target_points+" points written to file]");
	} else {
		System.err.println("#Anchor Points Used: "+target_points+"/"+universe_size+" ("+((double)target_points/(double)universe_size)*100.0+"%)");
	}
	
	long target_points_shifted = target_points + (target_points / 3)+2; //upsize the hashSet
	HashSet<Integer> pointsbox = new HashSet<Integer>((int)target_points_shifted);
	
	Random generator = new Random();
	long partial_target = target_points;
	do{
	
	for(long i = 0; i < partial_target; i++){
		if(string_length < 31){
			pointsbox.add(generator.nextInt((int)universe_size));
		} else if (string_length == 31){
			pointsbox.add((int) StrictMath.abs(generator.nextInt()));
		} else if (string_length == 32){
			pointsbox.add(generator.nextInt());
		}
	}
	if (string_length == 31){
		pointsbox.remove(Integer.MIN_VALUE);
	}
	partial_target = target_points - pointsbox.size();
	} while(pointsbox.size() < target_points);
	
	if(string_length <= 8){
		System.err.println("Anchor Points - for this run:");
		if(reverse_mode){ System.err.println("NOTE: Reverse mode active - all active points are the ones NOT in the set below.");}
		System.err.println(pointsbox);
	}
	
	//store all items
	conf.setBoolean("fuzzyJAnchor.reverseAnchorPoints", reverse_mode);
	conf.setLong("fuzzyJAnchor.numAnchorPoints", pointsbox.size());
	
	long points_stored = 0;
	
	FileSystem fs = FileSystem.get(conf);
	//apFile = fs.create(new Path("/temp/aptable.apt"), true); //overwrite existing AP table
	NullWritable key = NullWritable.get();
	IntWritable value = new IntWritable();
	SequenceFile.Writer writer = SequenceFile.createWriter( fs, conf, new Path("/temp/aptable.apt"), key.getClass(), value.getClass());
	for(int point: pointsbox){
		value.set(point);
		writer.append(key, value);
		points_stored++;
	}
	writer.close();
	
	/*for(int point : pointsbox){
		conf.setInt("fuzzyJAnchor.point"+points_stored, point);
		points_stored++;
	}*/
	

	Job job = new Job(conf, "Fuzzy Join [Binary Strings, Anchor Points alg.: threshold "+threshold+", "+string_length+"-bit universe].");
	
	
	
	
	job.setNumReduceTasks(Integer.parseInt(otherArgs[3]));
	//job.setNumReduceTasks(0);
	job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
	job.setJarByClass(FuzzyJoinAnchorNewBin.class);
	job.setMapperClass(FuzzyJoinAnchorNewBinMapper.class);
	job.setPartitionerClass(StringToIntPartitioner.class);
	//reducer's set below
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(MetadataIntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job, in_path);
	if(otherArgs[1].equals("null")){
		System.err.println("Note: ALL OUTPUT DISCARDED");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinAnchorNewBinReducer.class);
	} else if (otherArgs[1].equals("null-cost")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (measuring communication cost)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinAnchorNullReducer.class);
	} else if (otherArgs[1].equals("null-absolute")) {
		System.err.println("Note: ALL OUTPUT DISCARDED (completely)");
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.NullOutputFormat.class);
		job.setReducerClass(FuzzyJoinAnchorNewBinReducer.class);
	} else {
		job.setReducerClass(FuzzyJoinAnchorNewBinReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	}
	

	boolean success = job.waitForCompletion(true);
	if(!success){ System.exit(1); }
	System.exit(0);
  }
}
