Concepts - Higher-order concept discovery for Scala/Spark
===========================================================

*Concepts* is a library for calculating object correlation, similarity, and higher-order concepts in very large data sets. It aims at being a straightforward, easy-to-use and complete solution for
  * Calculation of pairwise concept correlations using a number of correlation measures
  * Perform efficient transformation from a correlation- to a similarity graph
  * Perform (different types of) graph-clustering for higher order concept discovery
  * Based on the above, efficiently perform hierarchical discovery of both recurrent, AND and OR concepts

The implementation aims to be
  * Fully scalable both in terms of input data, concepts, and discovered higher order concepts
  * Fully modular in terms of correlation measures, similarity measures, clustering algorithms etc.
  * Lightweight, based on a functional codebase where core functions have no side effects
  * Potentially portable to other compute engines such as Flink

## Basic definitions and assumptions

Let us start with some basic definitions:
  1. A *context* is an example where several *objects* occur together, such as the words in a tweet or the songs in a playlist.
  2. From object co-occurrences, we can build a *correlation graph* over all objects using some correlation measure.
  3. This correlation graph can be transformed to a *similarity graph* by measuring the similarity in the distribution of correlations for all object pairs.
  4. By performing clustering in these graphs, we can find structure that represent *higher order concepts*:
    1. In the correlation graph, clusters represent things that often occur together, i.e. an *AND concept* triggered when all of its constituents occur together.
    2. In the similarity graph, clusters represent things that are interchangeable, i.e. *OR concepts* triggered when any of its constituents occur.
  5. These concepts can be built hierarchically, i.e. OR concepts of AND concepts of OR concepts etc.
  6. As *recurrent concepts* or repetitions of the same object (see e.g. "no", "no no", "no no no") cannot be represented in the correlation graph, we identify and represent these as separate objects.

This library contains implementations for all of the calculations above, along with associated utility functions.

The library relies on a few basic assumptions:
  * Representations (data sets, clusters, higher-order concepts etc.) are stored and exchanged as Spark RDDs to allow for scaling.
  * All concepts, both basic (e.g. words, songs, codons, events) and higher-order, are identified by a unique *concept ID* represented by a `Long` value.
  * In all core library functions, data is represented by *(DataIndex, conceptID)* pairs indicating that *conceptID* occurred at example *DataIndex* in data. Just like the *conceptID*, the *DataIndex* is represented by a `Long`.
  * Clusters and higher-order concepts are represented as collections of the constituent concept IDs.

## Examples


## Further development

### Todo
  * Introduce passive concepts
  * Write and concept sizes in conceptdata
  * Thorough scalability testing, add caching where useful
  * Investigate suitable partitioners for critical calculations, make sure we preserve partitioning info where possible
  * Investigate level of parallelism

### Open issues
  * Basic efficiency tweaking of findHigherOrderConcepts

## Dependencies

The core library dependencies are restricted to
  * Spark (https://spark.apache.org/): All core functionality
  * Hadoop HDFS (http://hadoop.apache.org/docs/r2.7.1/): Some file operations
  * Kryo (https://github.com/EsotericSoftware/kryo): Spark serialization
  * Play JSON (https://www.playframework.com/documentation/2.2.x/ScalaJson):
    JSON IO functionality

For testing and development, additional dependencies include
  * Scalatest (http://www.scalatest.org): Unit testing
  * Scalastyle (http://www.scalastyle.org): Style checking
  * scopt (https://github.com/scopt/scopt): Command line options parsing

# Getting started

The cncpts library needs Scala 2.11 to work. If you do not already run Spark compiled for Scala 2.11, here's what you need to do:
  1. Download and rebuild Spark to work with Scala 2.11:
    1. Download the sources from [here](https://spark.apache.org/downloads.html)
    2. Build with support for Scala 2.11 as shown [here](http://spark.apache.org/docs/latest/building-spark.html#building-for-scala-211):

      ```
      ./dev/change-scala-version.sh 2.11
      ./build/mvn -Pyarn -Phadoop-2.4 -Dscala-2.11 -DskipTests clean package
      ```

      Use the `mvn` command provided in the Spark distribution to avoid build problems.
  2. Run sbt and create a fat jar using the command `assembly`. This creates a jar file containing the application. On one of the last few lines the location of the jar should be printed. It should be created at: [project-root]/target/scala-2.11/concepts-assembly-0.4.jar
  3. Submit the jar to `spark-submit`

Optionally, to make the process of rebuilding the library and submitting jobs easier, you can
  1. Save the path to the assembled jar as an environment variable in e.g. your .bashrc:
  
     ```
     export SPARK_JAR=/path/to/higherorder/target/scala-2.11/concepts-assembly-0.4.jar
     ```

  2. Create an environment variable $SPARK_HOME which points to the root of the Spark installation, e.g.

     ```
     export SPARK_HOME=/home/spark-1.5.1
     ```

  3. Create an alias in e.g. .bash_aliases:

     ```
     alias concepts-submit='$SPARK_HOME/bin/spark-submit --class se.sics.concepts.main.ConceptsMain $SPARK_JAR'
     ```

  4. Configure your run environment through the file $SPARK_HOME/conf/spark-defaults.conf as shown [here](http://spark.apache.org/docs/latest/configuration.html). An example to get you started can be found in conf/spark-defaults.conf.template.
  5. You can now start running jobs by using e.g.

    ```
    concepts-submit contexts --inputfile path/to/input --outputpath path/to/output --mincount 0 --minreccount 100 --winsize 1
    ```
  
  You just need to make sure to rebuild the fat jar if you've made changes to the code before you submit.
