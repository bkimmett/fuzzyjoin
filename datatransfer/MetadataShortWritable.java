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

/** A WritableComparable for shorts, with an extra byte of metadata. */
public class MetadataShortWritable implements WritableComparable {
  private short value;
  private byte meta;

  public MetadataShortWritable() {meta = 0x00;}

  public MetadataShortWritable(short value) { set(value); meta = 0x00; }
  public MetadataShortWritable(short value, byte meta) { set(value); setMeta(meta); }

  /** Set the value of this MetadataShortWritable. */
  public void set(short value) { this.value = value; }
  public void setMeta(byte meta) { this.meta = meta; }

  /** Return the value of this MetadataShortWritable. */
  public short get() { return value; }
  public byte getMeta() { return meta; }

  public void readFields(DataInput in) throws IOException {
    value = in.readShort();
    meta = in.readByte();
  }

  public void write(DataOutput out) throws IOException {
    out.writeShort(value);
     out.writeByte(meta);
  }

  /** Returns true iff <code>o</code> is a MetadataShortWritable with the same value. */
  public boolean equals(Object o) {
    if (!(o instanceof MetadataShortWritable))
      return false;
    MetadataShortWritable other = (MetadataShortWritable)o;
    return this.value == other.value && this.meta == other.meta;
  }

  public int hashCode() {
    return ((int)value | ((int)meta << 16)) ;
  }

  /** Compares two MetadataShortWritables. */
  public int compareTo(Object o) {
    short thisValue = this.value;
    short thatValue = ((MetadataShortWritable)o).value;
    byte thisMeta = this.meta;
    int thatMeta = ((MetadataShortWritable)o).meta;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? (thisMeta<thatMeta ? -1 : (thisMeta==thatMeta ? 0 : 1)) : 1));
    
    
  }

  public String toString() {
    return value+" "+meta;
  }

  /** A Comparator optimized for MetadataShortWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(MetadataShortWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int thisValue = readUnsignedShort(b1, s1);
      int thatValue = readUnsignedShort(b2, s2);
      byte thisMeta = b1[s1+2];
      byte thatMeta = b2[s2+2];
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? (thisMeta<thatMeta ? -1 : (thisMeta==thatMeta ? 0 : 1)) : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(MetadataShortWritable.class, new Comparator());
  }
}

