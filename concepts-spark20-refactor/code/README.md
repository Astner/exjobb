# Running

> sbt run

# Tuning

If getting "java.lang.OutOfMemoryError" it is necessary to increase JVM's MaxPermSize:

> sudo nano `which sbt`

and add option "-XX:MaxPermSize=512M" (change memory as needed) to "exec java...".

"If there is less than 32 GB of RAM, set the JVM flag -XX:+UseCompressedOops to make pointers be four bytes instead of eight. Also, on Java 7 or later, try -XX:+UseCompressedStrings to store ASCII strings as just 8 bits per character.": http://spark.incubator.apache.org/docs/latest/tuning.html .

The MaxPermSize is the maximum size for the permanent generation heap, a heap that holds the byte code of classes and is kept separated from the object heap containing the actual instances. 

Max Heap Size (-Xmx) and MaxPermSize have to be set taking into consideration how much memory is needed by the application classes and instances, the total memory of the server and the memory needed by other applications. Another important point is that expanding the memory from the Min Heap Size (-Xms) is a costly operation.

# Serialization

Everything distributed by Spark must be serializable:

http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when

Add the java flag

-Dsun.io.serialization.extendedDebugInfo=true

to the above file to enable more detailed error messages regarding serialization.