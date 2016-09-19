package se.sics.concepts.core

import java.io.Serializable
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object MyCoreNLP {
  // Set up parser
  val Props = new Properties()
  Props.setProperty("annotators", "tokenize,ssplit")
  Props.setProperty("tokenize.language", "en")
  @transient lazy val coreNLP = new StanfordCoreNLP(Props)
}

class RDDBigramParser(source: RDD[String], splitSentences: Boolean, windowSize: Int = 2) extends Serializable {

  def sentenceSplit(text: String) = {
    val doc = new Annotation(text)
    MyCoreNLP.coreNLP.annotate(doc)
    // Split into sentences
    val sentences = doc.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences.toArray.map(_.toString).toTraversable
  }

  protected val interm: RDD[String] = {
    if (splitSentences) source.flatMap(sentenceSplit(_))
    else source
  }

  // Parse sets
  val stringSets: RDD[Array[String]] = interm
    .map(ln => ln.replaceAll("[^a-zA-Z0-9 ]", " ").toLowerCase.split(" +"))
    // Remove singletons
    .filter(_.size > 1)
    // Create word bigrams from defined context
    .flatMap(RDDBigramParser.context(_, windowSize))


  lazy val pairCounts = countPairs(stringSets)

  def countPairs(sets: RDD[Array[String]]): RDD[((String, String), Long)] = {
    // Generates all ordered pairs
    def pairs(s: Array[String]): Iterator[(String, String)] = s.combinations(2).map { case Array(a, b) => (a, b) }
    val bigramCountPairs: RDD[((String, String), Long)] = sets.flatMap(pairs(_)).map((_, 1L)).reduceByKey(_ + _)
    bigramCountPairs
  }
}

object RDDBigramParser {

  // Create context elements for each sentence
  // TODO: This is problematic for windowSize = 2, as it also creates unordered
  // pairs. We should handle this as a special case.
  def context(a: Array[String], n: Int): Array[Array[String]] = {
    // Create ngrams
    val ngrams = a.sliding(n).map(_.toArray).toArray
    //Create bigrams out of permutations of ngrams
    val bigrams = ngrams
      .flatMap(_.combinations(2))
      // TODO: Possible optimization: Map everything to tuple instead, avoid need to cast to genericWrapArray
      // Should be fine since each Array after the flatMap should have two elements, correct?
      .map(genericWrapArray(_))
      .distinct //TODO: Distinct takes a long time, how's the performance if we skip it?
      .map(_.toArray)
      .flatMap{case Array(a, b) => Array(Array(a, b), Array(b, a))}

    bigrams
  }

  def apply(
             source: RDD[String],
             splitSentences: Boolean = true,
             windowSize: Int = 2) = new RDDBigramParser(source, splitSentences, windowSize)
}
