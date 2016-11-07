
# Function to run any main class from concepts.main                                                                                       

SPARK_CONCEPTS_JAR="$HOME/exjobb/concepts-master/experimental/higherorder/target/scala-2.11/concepts-assembly-0.4.jar" 

HDFS_INPUT="hdfs://sprk1:9000/user/astner/ngram/fullFile/files"

#HDFS_FOLDER="hdfs://sprk1:9000/user/astner/ccm_files/run2"

TEST_FOLDER="/user/astner/ccm_files/run2"

LOCAL_FOLDER="/extra/data/astner/exjobb/runs/run2"

#SUBMITTER="/home/sprk/spark-1.6.1/bin/spark-submit"
#SUBMITTER=spark-submit

concepts-run ()
{
    CLASS="$1"
    shift
    args="$@"
    /home/sprk/spark-1.6.1/bin/spark-submit --class se.sics.concepts.main.$CLASS "$SPARK_CONCEPTS_JAR" $args
}

local-run ()
{
    CLASS="$1"
    shift
    args="$@"
    spark-submit --class se.sics.concepts.main.$CLASS "$SPARK_CONCEPTS_JAR" $args
}


#~/exjobb/yahoo/data/ngram/fullFile/files
#/extra/data/astner/CCMdata/run2 


#-----------------------------------------

#concepts-run ContextsMain -if "$HDFS_INPUT" -op "$TEST_FOLDER" -f ngrams -uq true -mcc 100

#concepts-run GraphsMain --help 

#concepts-run GraphsMain graphs -p "$TEST_FOLDER" -cf pwmi -ca all -md 300 -mcr 0.1 -msm 0.1

#local-run ConceptsMain statistics -lcl -p "$LOCAL_FOLDER" -ca all

local-run ConceptsMain export -lcl -p "$LOCAL_FOLDER" -d graphs -f json -me 6000000
