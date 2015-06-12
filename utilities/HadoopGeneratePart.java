import java.io.*;
import java.io.IOException;
import java.net.URI;
import java.lang.Integer;
import java.util.HashSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


//White, Tom (2012-05-10). Hadoop: The Definitive Guide (Kindle Locations 5375-5384). OReilly Media - A. Kindle Edition. 

public class HadoopGeneratePart { 
    public static void main( String[] args) throws IOException { 
    	if (args.length !=3) {
      System.err.println("Usage: HadoopGeneratePart <number of bits> <chance of point inclusion> <output filename>. Chance is a number N where the final set will be 1/N of the universe size.");
      System.exit(2);
    }    
    	int limit;
    	int limit_shifted;
        int numBits = Integer.parseInt(args[0]);
        if(numBits > 31){
			limit = (Integer.MAX_VALUE / Integer.parseInt(args[1]))*2; //expecting integer overflow, this circumvents it if the compiler doesn't get clever
			limit_shifted = limit;
        } else {
			limit = (1 << numBits)/Integer.parseInt(args[1]);
			limit_shifted = limit + (limit/3) + 1;
        }
         
         
         
         HashSet<Integer> pointsbox = new HashSet<Integer>((int)limit_shifted);
	
		Random generator = new Random();
		int partial_target = limit;
		long universe_size = 1 << numBits;
		do{
		System.err.println("Starting new pass: generating "+partial_target+" points.");
		for(long i = 0; i < partial_target; i++){
			
			if(numBits < 31){
				pointsbox.add(generator.nextInt((int)universe_size));
			} else if (numBits == 31){
				pointsbox.add((int) StrictMath.abs(generator.nextInt()));
			} else if (numBits == 32){
				pointsbox.add(generator.nextInt());
			}
		}
		if (numBits == 31){
			pointsbox.remove(Integer.MIN_VALUE);
		}
		partial_target = limit - pointsbox.size();
		} while(pointsbox.size() < limit);
         
         
         
         
         
         System.err.println("Limit: "+limit);
        String uri = args[2];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create( uri), conf);
        Path path = new Path( uri);
        NullWritable key = NullWritable.get();
        IntWritable value = new IntWritable();
        SequenceFile.Writer writer = null;
        int points_stored = 0;
        try { 
            writer = SequenceFile.createWriter( fs, conf, path, key.getClass(), value.getClass());
            int value_i = 0;
            for(int point : pointsbox){
				value.set(point);
				writer.append( key, value);
				points_stored++;
			}
       		System.err.println("Done. "+points_stored+" points written.");
      } finally { 
        	IOUtils.closeStream( writer); 
       } 
    } 
}

