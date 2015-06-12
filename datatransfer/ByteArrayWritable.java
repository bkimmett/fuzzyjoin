package ca.uvic.csc.research;

import java.io.IOException;
import java.io.DataInput; // needed EXCLUSIVELY for ByteArrayWritable
import java.io.DataOutput;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.WritableComparable;

/* This is a MapReduce class that describes a data structure for passing arrays of integers between MapReduce processes.
It behaves similarly to the regular MapReduce ArrayWritable class. */


public class ByteArrayWritable implements WritableComparable<ByteArrayWritable> { 
		private ByteWritable[] values;
		
		public ByteArrayWritable() { 
		}
		public ByteArrayWritable(int length) {
			this.values = new ByteWritable[length];
		} 
		public ByteArrayWritable(ByteWritable[] values) {
        	this.values = values;
   		}
   		
   		// set() lets you pass in an entire array (of ByteWritables-- not integers!), or just one index to change and the value to change it to.
   		
   		public void set(ByteWritable[] values) { this.values = values; }
		public void set(int index, ByteWritable value) { this.values[index] = value; }
		public void set(int index, int value) { this.values[index] = new ByteWritable((byte)value); }
		
		// get() lets you get the entire array, or just one index. getInt() lets you skip typecasting and get the integer value of one array element.
		
  		public ByteWritable[] get() { return values; }
  		public ByteWritable get(int index) { return values[index]; }
  		public int getInt(int index) { return (values[index]).get(); }
  		
  		// these are needed for mapReduce.
  		
  		public void readFields(DataInput in) throws IOException {
			values = new ByteWritable[in.readShort()];          // construct values
			for (int i = 0; i < values.length; i++) {
			  values[i] = new ByteWritable(in.readByte());                          // read a value + store it in values
			}
		  }

		 public void write(DataOutput out) throws IOException {
			out.writeShort((short)values.length);                 // write values
			for (int i = 0; i < values.length; i++) {
			  values[i].write(out);
			}
		  }
		  
		  //compareTo: First value in the two arrays that's different wins by regular <, >.
		  
		  public int compareTo(ByteArrayWritable o){
		  	int j;
		  	for (int i = 0; i < values.length; i++) {
		  		j = values[i].get() - o.values[i].get();
				if(j != 0){return j;}
			}
			return 0;
		  }
		  public String toString(){
		  	String result = "";
		  	int len = values.length;
		  	for (int i = 0; i < len; i++) {
		  		result += values[i].get();
		  		if(i < len-1){
		  			result += " ";
		  		}
			}
			return result;
		  }
		  
		  //this returns an array of integers [of the ByteArrayWritable's contents].
		  
		  public int[] toIntArray(){
		  	int[] result = new int[values.length];
		  	for (int i = 0; i < values.length; i++) {
		  		result[i] = values[i].get();
			}
		  	return result;
		  }
	}