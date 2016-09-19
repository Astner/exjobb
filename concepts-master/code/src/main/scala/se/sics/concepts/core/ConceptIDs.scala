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

package se.sics.concepts.core

/** Concept ID range definitions and functions for identifying different types of concepts. */
object ConceptIDs {
  /** Type alias for concept IDs, used to increase readability. */
  type ConceptID    = Long
  /** Type alias for cluster IDs, used to increase readability. */
  type ClusterID    = Long
  /** Type alias for data indices, used to increase readability. */
  type DataIndex    = Long
  /** Type alias for concept counts, used to increase readability. */
  type ConceptCount = Long
  /** Type alias for concept pair counts, used to increase readability. */
  type PairCount = Long
  /** Type alias for edge weights, used to increase readability. */
  type Weight = Double

  /** Start ID for recurrent concepts. */
  val recurrentConceptsStartID  : ConceptID = 100000000000L
  /** Start ID for AND concepts. */
  val andConceptsStartID  : ConceptID       = 200000000000L
  /** Start ID for OR concepts. */
  val orConceptsStartID   : ConceptID       = 300000000000L

  /** Return true iff the concept id belongs to a basic (not higher order) concept. */
  def isBasicConcept(id: ConceptID)     : Boolean =
    id < recurrentConceptsStartID
  /** Return true iff the concept id belongs to a recurrent concept. */
  def isRecurrentConcept(id: ConceptID) : Boolean =
    (id >= recurrentConceptsStartID && id < andConceptsStartID)
  /** Return true iff the concept id belongs to an AND concept. */
  def isANDConcept(id: ConceptID)       : Boolean =
    (id >= andConceptsStartID && id < orConceptsStartID)
  /** Return true iff the concept id belongs to an OR concept. */
  def isORConcept(id: ConceptID)        : Boolean =
    id >= orConceptsStartID
}
