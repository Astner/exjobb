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

import se.sics.concepts.graphs.ConceptIDs
import se.sics.concepts.higherorder.Formula.{flattenCNF, toCNF, minimizeCNF}
import ConceptIDs._

/** Higher order (logical) concept representation.
  *
  * @param id Concept identifier.
  * @param formula Formula representation.
  */
case class HigherOrderConcept(id: ConceptID, formula: Formula) {
  /** CNF representation. */
  val cnf: Set[Set[ConceptID]] = minimizeCNF(flattenCNF(toCNF(formula)))

  /** Custom "==" definition, e.g. to enable use of distinct on RDDs. */
  override def equals(other: Any): Boolean = {
    other match {
      case other: HigherOrderConcept => cnf == other.cnf
      case _ => false
    }
  }

  /** Use truncated index as hash code. NOTE: hash codes restricted to 32 bits. */
  override def hashCode: Int = id.toInt

  /** Enable println etc. */
  override def toString: String =
    id.toString+": ["+cnf.map("("+_.mkString(",")+")").mkString(",")+"]"
}

/** Companion object for class [[HigherOrderConcept]]. */
object HigherOrderConcept {
  def apply(i: Long) : HigherOrderConcept = new HigherOrderConcept(i,Atom(i))
}
