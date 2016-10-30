
# Function to run any main class from concepts.main                                                                                       

SPARK_CONCEPTS_JAR="$HOME/exjobb/concepts-spark20-refactor/experimental/higherorder/target/scala-2.11/concepts-assembly-0.4.jar"

HDFS_INPUT ="hdfs://sprk1:9000/user/astner/ngram/fullFile"


SUBMITTER=$submit
#SUBMITTER=spark-submit

concepts-run ()
{
    CLASS="$1"
    shift
    args="$@"
    SUBMITTER --class se.sics.concepts.main.$CLASS "$SPARK_CONCEPTS_JAR" $args
}





concepts-run ContextsMain -lcl -if ~/exjobb/yahoo/data/ngram/fullFile/files -op /extra/data/astner/CCMdata/run2 -f ngrams -uq true -mcc 100

#concepts-run GraphsMain

#concepts-run ConceptsMain statistics -lcl -p /extra/data/astner/CCMdata/run1 -ca all
