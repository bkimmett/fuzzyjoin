# MapReduce Fuzzy Join Algorithms


## ABOUT

This is the set of fuzzy join algorithms tested in the paper, 'Three-way joins on MapReduce: An experimental study' by B. Kimmett, A. Thomo, and V. Srinivasan. (doi: 10.1109/IISA.2014.6878811)

The algorithms each execute a fuzzy join on strings of bits less than or equal to 32 bits, represented as integers in a Hadoop .seq file. Output is textual binary in order to be human-readable.

### List of all files/folders:

+ FuzzyJoin*Bin.java - source for the different Fuzzy Join workflows. Ball-3 is the optimized version of the Ball-Hashing 1 algorithm from the paper.

+ utilities/CompareTwoOutputs.java - source for a tool that compares two outputs to find out if they’re identical. If there is a mismatch, it’ll say what mismatches in its output. If they are an exact match, output will be empty. 
+ utilities/HadoopGenerateFull.java - source for a tool that creates a .seq (input) file corresponding to a dataset of all possible bit strings of a specific length.
+ utilities/HadoopGeneratePart.java - source for a tool that creates a .seq (input) file corresponding to a randomized dataset that is a fraction of the universe of all possible bit strings of a specific length.

+ datatransfer/*.java - source for custom Writable (data-transfer) formats needed by various of the algorithms.

+ jar/*.jar - compiled versions of all of the above source files.

+ ca/uvic/csc/research/*.class - The compiled components that went into the above JAR files. The somewhat awkward folder structure is because of the package name used.

+ LICENSE - This code is using the MIT license, so you can do absolutely anything with it as long as this file comes along.

+ README.md - This file.


## COMPILING
*Please note that the default GitHub package includes precompiled jar files in the jar/ folder. You shouldn't need this section most of the time.*

To compile these programs, you’ll need an installation of Hadoop. The algorithms were made for Hadoop 1.2.1, but other 1.x versions will probably run them.

First, cd to the directory that houses this file:

### Compiling the data transfer classes [needed for the next bits]:

javac -classpath path_to_your_hadoop_install/hadoop-core-1.2.1.jar -d ca/uvic/csc/research/ datatransfer/*Writable.java 

The version numbers and ‘path_to_your_hadoop_install’ may vary depending on the release and location of your Hadoop installation.


### Compiling the Hadoop jobs:

javac -classpath path_to_your_hadoop_install/hadoop-core-1.2.1.jar:path_to_your_hadoop_install/lib/commons-cli-1.2.jar:. -d . FuzzyJoin*.java


### Compiling the utilities:

javac -classpath path_to_your_hadoop_install/hadoop-core-1.2.1.jar:path_to_your_hadoop_install/lib/commons-cli-1.2.jar:. -d . utilities/CompareTwoOutputs.java

javac -classpath path_to_your_hadoop_install/hadoop-core-1.2.1.jar -d ca/uvic/csc/research utilities/HadoopGenerateFull.java

javac -classpath path_to_your_hadoop_install/hadoop-core-1.2.1.jar -d ca/uvic/csc/research utilities/HadoopGeneratePart.java

### Packing everything up in .jar files:

#### Fuzzy Join - Naive

jar cvf jar/FuzzyJoinNaiveBin.jar ca/uvic/csc/research/FuzzyJoinNaiveBin* ca/uvic/csc/research/ByteArrayWritable.class ca/uvic/csc/research/MetadataIntWritable* 

#### Fuzzy Join - Ball-Hashing

jar cvf jar/FuzzyJoinBall2Bin.jar ca/uvic/csc/research/FuzzyJoinBall2Bin*

jar cvf jar/FuzzyJoinBall3Bin.jar ca/uvic/csc/research/FuzzyJoinBall3Bin*

#### Fuzzy Join - Splitting

jar cvf jar/FuzzyJoinSplitBin.jar ca/uvic/csc/research/FuzzyJoinSplitBin* ca/uvic/csc/research/MetadataShortWritable* 

#### Fuzzy Join - Anchor Points

jar cvf jar/FuzzyJoinAnchorNewBin.jar ca/uvic/csc/research/FuzzyJoinAnchorNewBin* ca/uvic/csc/research/MetadataIntWritable* 

#### Output Comparator

jar cvf jar/CompareTwoOutputs.jar ca/uvic/csc/research/CompareTwoOutputs* ca/uvic/csc/research/IntArrayWritable.class

#### Input File Generators

jar cvf jar/HadoopGenerateFull.jar ca/uvic/csc/research/HadoopGenerateFull.class

jar cvf jar/HadoopGeneratePart.jar ca/uvic/csc/research/HadoopGeneratePart.class


## USAGE

All commands are meant to be run from your hadoop directory.
To run the fuzzy join algorithms, it's more or less always the same command:

bin/hadoop jar `<workflow>`.jar ca.uvic.csc.research.`<workflow>` -D mapred.max.split.size=`<split>` `<options>` `<input>` `<output>` `<threshold>` `<settings>`

`<workflow>` is one of _FuzzyJoinNaiveBin_ (Naive), _FuzzyJoinBall2Bin_ or _FuzzyJoinBall3Bin_ (Ball-Hashing), _FuzzyJoinSplitBin_ (Splitting), or _FuzzyJoinAnchorNewBin_ (Anchor Points). This assumes you have the jar file in Hadoop's root folder. If it's elsewhere, you may need to modify the path accordingly.

`<split>` specifies the maximum size of a segment of input. Set this to (size of input file)/(number of mappers desired) bytes, rounding up.

`<options>` is a placeholder for some extra '-D' lines that may be added depending on algorithm.
+ Due to the way the Naive algorithm operates, some reducers may time out due to the way (and the amount of time taken as) they compute data. If this occurs, add `-D mapred.task.timeout=1800000` to extend the timeout range.
+ `-D mapred.compress.map.output=true` may be added to any workflow to compress intermediate output.

`<input>` should be the location in the HDFS of the input to the join algorithm. 

`<output>` should be the output location in HDFS; it also accepts several special values. 'null', 'null-cost', and 'null-absolute' all cause no output to be saved, but there are slight differences in what happens under the hood:
+ 'null' emits reducer output, using Hadoop's NullOutputFormat. Data will be emitted but not written.
+ 'null-absolute' does _not_ emit reducer output, instead incrementing a variable of records emitted. This variable is not given to Hadoop, and instead serves to prevent the possibility of the output step being compiled out entirely.
+ 'null-cost' replaces the workflow's reducer class with a dummy class that ignores all input it receives. No reducer processing will be performed; the reducer will exit once all its input has been read.
	These special values are intended for use in testing and benchmarking.
	
`<threshold>` determines the maximum number of bits that may be different between two strings of bits for them to be deemed as similar.
	
`<settings>` must be included for each algorithm, but they vary depending on which is used:
+ For the Naive algorithm, there is one value: `<granularity>`. This value represents the number of reducers to be used; if granularity is some (n), then ((n)\*(n+1))/2 reducers will be used. Higher granularity values increase paralellism and should result in faster job completion; this comes at the cost of increasing communication cost proportional to the granularity value (it acts as a linear multiplier). In practice, the communication cost is typically smalller than the reducer processing time for this algorithm, so the granularity should be set to the value where it uses as many of your reducers as possible.
+ For all other algorithms, there are two values: `<num_reducers>` and `<universe_size>`. `<num_reducers>` tells the partitioner how many parts to divide the mapper output into; this number should be equal, or slightly less than, the number of reducers you have. `<universe_size>` sets the size of the universe of strings that the algorithm should check, and may be used to restrict the algorithms to universes of strings smaller than 32 bits. Do *not* set this value to 32 and ignore it if you are using a smaller universe, for horrible slowdowns await.	
	
	
## DATA GENERATION AND VALIDATION

All the input files are expected to be in the form of "sequence file with Null/Integer key/value pairs, representing binary strings". The generator utilities can create these files for you.

### Generating all strings of a certain length

_HadoopGenerateFull_ creates a set that contains the entire universe of binary strings that can be represented in the number of bits you specify:

bin/hadoop jar HadoopGenerateFull.java HadoopGenerateFull `<number of bits>` `<output>`

`<number of bits>` should be the length of the binary strings that the workflow will create. Note that no matter what, these binary strings will be _stored_ in 32-bit integers; if you enter a number higher than '32', the generator will create the universe of all binary strings that can be represented with 32 bits.

`<output>` should be the output location in HDFS for your created file. A typical output location would be where your algorithms will look for their input.

### Generating a random set of strings of a certain length

_HadoopGeneratePart_ creates a set that contains a random selection of binary strings that can be represented in the number of bits you specify:

bin/hadoop jar HadoopGeneratePart.java HadoopGeneratePart `<number of bits>` `<fraction of universe>` `<output>`

`<number of bits>` and `<output>` are the same as above.

`<fraction of universe>` is a number that acts as a denominator for a fraction that denotes what part of the universe of binary strings to put in the set. So, if you enter '2' for this number, the final set will be 1/2 the magnitude of the set that would be generated by HadoopGenerateFull on the same `<number of bits>` setting. If you enter '3', the final set will be 1/3 the magnitude of the HadoopGenerateFull set, and so on.
The points in the final set are chosen randomly.

### Checking two output files are the same

_CompareTwoOutputs_ takes two output paths as input, then reads in all output records and compares them. It expects textual input (that is, pairs of white-space-separated numbers in textual form) and gives textual output.

bin/hadoop jar CompareTwoOutputs.jar ca.uvic.csc.research.CompareTwoOutputs `<input 1>` `<input 2>` `<output>` `<number of reducers>`

`<input 1>` and `<input 2>` are the locations, in HDFS, of the output you want to compare.

`<output>` is a location, in HDFS, to store the result of the comparison. In an ideal universe, this will be empty (indicating both inputs are identical). If this is not the case, the output will contain detailed descriptions of how the files differ.
The comparator will also catch if the same value pair is duplicated in one of the inputs, and issue a warning to the output.

`<number of reducers>` tells the comparator how many reducers to use when dividing your comparison. This should be set to a comfortable number for your Hadoop installation.
