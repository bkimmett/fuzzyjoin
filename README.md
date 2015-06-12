# MapReduce Fuzzy Join Algorithms


## ABOUT

This is the set of fuzzy join algorithms _______ [explain about the paper]

The algorithms each execute a fuzzy join on strings of bits less than or equal to 32 bits, represented as integers in a Hadoop .seq file. Output is textual binary in order to be human-readable.

### List of all files/folders:

+ FuzzyJoin*Bin.java - source for the different Fuzzy Join workflows. Ball-3 is the optimized version of the Ball-Hashing 1 algorithm from _________ paper.

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

