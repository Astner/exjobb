#For running localy

SPARK_CONCEPTS_JAR="$HOME/exjobb/concepts-master/experimental/higherorder/target/scala-2.10/concepts-assembly-0.4.jar"
#/home/niklas/exjobb/concepts-master/experimental/higherorder/target/scala-2.10/concepts-assembly-0.4.jar


INPUT="/home/niklas/exjobb/yahoo/data/ngram/smallSubset/files"

LOCAL_FOLDER="/home/niklas/exjobb/yahoo/data/experiments/runB2"



local-run ()
{
    CLASS="$1"
    shift
    args="$@"
    spark-submit --class se.sics.concepts.main.$CLASS "$SPARK_CONCEPTS_JAR" $args
}



local-run ContextsMain -if "$INPUT" -op "$LOCAL_FOLDER" -f ngrams -uq true -mcc 8

local-run GraphsMain graphs -p "$LOCAL_FOLDER" -cf pwmi -ca all -md 300 -mcr 0.1 -msm 0.1

local-run ConceptsMain statistics -lcl -p "$LOCAL_FOLDER" -ca all

local-run ConceptsMain export -lcl -p "$LOCAL_FOLDER" -d graphs -f json -me 6000000
