/*
 Copyright (C) 2015 Daniel Gillblad, Olof Görnerup , Theodoros Vasiloudis (dgi@sics.se,
 olofg@sics.se, tvas@sics.se).

 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package se.sics.concepts.higherorder

/** Basic logical formula class. */
abstract class Formula
/** Atomic expression.
  *
  * @param id Identifier.
  */
case class Atom(id: Long) extends Formula
/** And expression.
  *
  * @param f Left hand expression.
  * @param g Right hand expression.
  */
case class And(f: Formula, g: Formula) extends Formula
/** Or expression.
  *
  * @param f Left hand expression.
  * @param g Right hand expression.
  */
case class Or(f: Formula, g: Formula) extends Formula

/** Logical formula related functionality. */
object Formula {

  /** Apply distributive law to expressions. */
  def distribute(f: Formula, g: Formula): Formula = {
    (f,g) match {
      case (And(p,q), formula) => And(distribute(p, formula), distribute(q, formula))
      case (formula, And(p,q)) => And(distribute(formula, p), distribute(formula, q))
      case _ => Or(f, g)
    }
  }

  /** Convert formula to conjunctive normal form. */
  def toCNF(f: Formula): Formula = {
    f match {
      case Atom(i) => f
      case And(p,q) => And(toCNF(p), toCNF(q))
      case Or(p,q) => distribute(toCNF(p), toCNF(q))
    }
  }

  /** Flatten Or expressions. */
  def flattenOrs(f: Formula): Set[Long] = {
    f match {
      case Atom(i) => Set(i)
      case Or(p,q) => flattenOrs(p)++flattenOrs(q)
    }
  }

  /** Flatten formula on conjunctive normal form. */
  def flattenCNF(f: Formula): Set[Set[Long]] = {
    f match {
      case Atom(i) => Set(Set(i))
      case Or(p,q) => Set(flattenOrs(p)++flattenOrs(q))
      case And(p,q) => flattenCNF(p)++flattenCNF(q)
    }
  }

 /** Minimizes CNF by removing subsumed clauses.
  *
  * Based on Algorithm 1 in Zhang, L. On Subsumption Removal and 
  * On-the-Fly CNF Simplification. SAT, pp 482-489. 2005.
  * @param clauses formula in conjunctive normal form.
  */
  def minimizeCNF(clauses: Set[Set[Long]]) = {
    val indexedClauses = clauses.zipWithIndex

    // Sets of clauses per literals (reverse map)
    val clauseSets: Map[Long, Set[Int]] = 
                     indexedClauses.
                     // Associate each literal with clauses
                     flatMap{case (s,i) => s.map(l => (l,i))}.
                     // Group clauses per literal
                     groupBy{case (l,i) => l}.
                     // Discard redundant literal keys
                     map{case (l,s) => (l, s.map{case (i,j) => j})}

    val clauseIndices = Range(0,clauses.size).toSet

    // Returns indices of clauses subsumed by clause
    def subsumedClauses(indexedClause: (Set[Long], Int)): Set[Int] = {
      var s = clauseIndices - indexedClause._2
      for(literal <- indexedClause._1){
        s = s.intersect(clauseSets(literal))
        if(s.isEmpty) return s
      }
      s
    }

    // Collect indices of subsumed clauses
    val subsumed = indexedClauses.flatMap(subsumedClauses)

    // Return non-subsumed clauses
    indexedClauses.filter{case (c,i) => !subsumed.contains(i)}.map{case (c,i) => c}
  }
}
