import java.io.*;
import java.io.IOException;
import java.net.URI;
import java.lang.Integer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

//White, Tom (2012-05-10). Hadoop: The Definitive Guide (Kindle Locations 5375-5384). OReilly Media - A. Kindle Edition. 

public class HadoopGenerateFull { 
    public static void main( String[] args) throws IOException { 
    	if (args.length !=2) {
      System.err.println("Usage: HadoopGenerateFull <number of bits>  <output filename>.");
      System.exit(2);
    }    
    	int limit;
        int numBits = Integer.parseInt(args[0]);
        if(numBits > 31){
			limit = 0; //expecting integer overflow
        } else {
			limit = (1 << numBits);
         }
         System.err.println("Limit: "+limit);
        String uri = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create( uri), conf);
        Path path = new Path( uri);
        NullWritable key = NullWritable.get();
        IntWritable value = new IntWritable();
        SequenceFile.Writer writer = null;
        try { 
            writer = SequenceFile.createWriter( fs, conf, path, key.getClass(), value.getClass());
            int value_i = 0;
            do {
            	value.set(value_i);
            	//System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
            	writer.append( key, value);
            	value_i++;
       		} while(value_i != limit);
       		System.err.println("Done.");
      } finally { 
        	IOUtils.closeStream( writer); 
       } 
    } 
}

