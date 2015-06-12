/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uvic.csc.research;

import java.io.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** A WritableComparable for ints, with an extra byte of metadata.. */
public class MetadataIntWritable implements WritableComparable {
  private int value;
  private byte meta;

  public MetadataIntWritable() {meta = 0x00;}

  public MetadataIntWritable(int value) { set(value); meta = 0x00; }
  public MetadataIntWritable(int value, byte meta) { set(value); setMeta(meta); }

  /** Set the value of this MetadataIntWritable. */
  public void set(int value) { this.value = value; }
  public void setMeta(byte meta) { this.meta = meta; }

  /** Return the value of this MetadataIntWritable. */
  public int get() { return value; }
  public byte getMeta() { return meta; }

  public void readFields(DataInput in) throws IOException {
    value = in.readInt();
    meta = in.readByte();
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(value);
     out.writeByte(meta);
  }

  /** Returns true iff <code>o</code> is a MetadataIntWritable with the same value. */
  public boolean equals(Object o) {
    if (!(o instanceof MetadataIntWritable))
      return false;
    MetadataIntWritable other = (MetadataIntWritable)o;
    return this.value == other.value && this.meta == other.meta;
  }

  public int hashCode() {
    return value;
  }

  /** Compares two MetadataIntWritables. */
  public int compareTo(Object o) {
    int thisValue = this.value;
    int thatValue = ((MetadataIntWritable)o).value;
    byte thisMeta = this.meta;
    int thatMeta = ((MetadataIntWritable)o).meta;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? (thisMeta<thatMeta ? -1 : (thisMeta==thatMeta ? 0 : 1)) : 1));
    
    
  }

  public String toString() {
    return value+" "+meta;
  }

  /** A Comparator optimized for MetadataIntWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(MetadataIntWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int thisValue = readInt(b1, s1);
      int thatValue = readInt(b2, s2);
      byte thisMeta = b1[s1+4];
      byte thatMeta = b2[s2+4];
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? (thisMeta<thatMeta ? -1 : (thisMeta==thatMeta ? 0 : 1)) : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(MetadataIntWritable.class, new Comparator());
  }
}

