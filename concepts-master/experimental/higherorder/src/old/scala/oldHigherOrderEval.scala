package com.sics.concepts

import scala.reflect._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.sics.concepts.Concepts._
import com.sics.concepts.CorrelationMeasures._

object timer {
  def time[T](str: String)(thunk: => T): T = {
    print(str + "... ")
    val t1 = System.currentTimeMillis
    val x = thunk
    val t2 = System.currentTimeMillis
    println((t2 - t1) + " msecs")
    x
  }
}

object HigherOrderEval {
  val rand = new scala.util.Random

  def randomConceptSet(maxId : Long, size : Int, initialSet : Set[Long] = Set()) : Set[Long] = {
    if(size <= 0) initialSet
    else randomConceptSet(maxId, size - 1, initialSet + rand.nextInt(maxId.toInt).toLong)
  }

  def randomGeometricLenConceptSet(maxId : Long, p : Double) : Set[Long] = {
    randomConceptSet(maxId, 2 + Math.floor(Math.log(rand.nextDouble()) / (Math.log(1 - p))).toInt)
  }

  def generateConceptSets(nrSets: Int, maxId : Long, p : Double,
                          setList : List[Set[Long]] = Nil) : List[Set[Long]] = {
    if(nrSets <= 0) setList
    else generateConceptSets(nrSets - 1, maxId, p,
                             randomGeometricLenConceptSet(maxId, p) :: setList)
  }

  def generateHigherOrderConcepts(maxPrimitiveId : Long, nrPhases : Int,
                                  higherOrderFrac : Double, p : Double) = {
    val nrSets : Int = (higherOrderFrac * maxPrimitiveId.toDouble / nrPhases.toDouble).toInt
    (1 to nrPhases).toList.map((x) => {
      generateConceptSets(nrSets, maxPrimitiveId + (x - 1) * nrSets, p)
      }).reduce((x : List[Set[Long]], y : List[Set[Long]]) => x ::: y)
    .zipWithIndex.map((x)=>((x._2 + maxPrimitiveId), x._1))
  }

  def findAndActivations(activations : Set[Long], andConcepts : RDD[(Long, Set[Long])]) : Set[Long] = {
    val c = andConcepts.filter((x)=>{ x._2 subsetOf activations})
    activations
  }

  def findOrActivations(activations : Set[Long], orConcepts : RDD[(Long, Set[Long])]) : Set[Long] = {
    val c = orConcepts.filter((x)=>(x._2 intersect activations).nonEmpty)
    // The following is typically slower, especially for the most common case of no intersection
    //val c = orConcepts.filter((x)=>{ (x._2.exists((y)=>activations.contains(y))) })

    activations
  }

  def testGenerateConcepts() = {
    val nrActivationReps    : Int    = 100000000
    val contextSize         : Int    = 5
    val nrPrimitiveConcepts : Long   = 100
    val nrPhases            : Int    = 3
    val andConceptsFrac     : Double = 0.1
    val orConceptsFrac      : Double = 0.2
    val randomLengthP       : Double = 0.5

    val andConcepts =
      ConceptsEval.generateHigherOrderConcepts(nrPrimitiveConcepts, nrPhases, andConceptsFrac, randomLengthP)
    val orConcepts  =
      ConceptsEval.generateHigherOrderConcepts(nrPrimitiveConcepts + andConcepts.size.toLong,
                                       nrPhases, orConceptsFrac, randomLengthP)
    val nrConcepts = nrPrimitiveConcepts + andConcepts.size.toLong + orConcepts.size.toLong

    println("Number of AND concepts: " + andConcepts.size)
    println("Number of OR concepts: " + orConcepts.size)

    timer.time("Testing activation A") {
      for (i <- 1 to nrActivationReps) {
        //HigherOrder.findOrActivationsTA(act, cl)
      }
    }
  }
}
