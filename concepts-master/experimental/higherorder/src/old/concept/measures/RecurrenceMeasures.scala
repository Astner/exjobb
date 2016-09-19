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

package se.sics.concepts.measures

/** Recurrence measures on standard format for recurrent concept derivation.
*
* Each recurrence measure takes at least three arguments (used as interface from other
* functions): the number of times the concept
* has occurred with a certain number of repetitions, the number of examples the concept
* has occurred in at all, the total number of examples in the data set, and, depending on the
* measure, 0 or more parameters. These parameters are typically bound, passing the resulting
* expression on standard format.
*/
object RecurrenceMeasures {
  /** Returns true if the recurrence has existed at all. */
  def existence(recurrenceCount : Long, conceptCount : Long, nrExamples : Long) : Boolean =
    recurrenceCount >= 1L

  /** Returns true if the recurrence always occurs when the concept occurs. */
  def always(recurrenceCount : Long, conceptCount : Long, nrExamples : Long) : Boolean =
    recurrenceCount >= conceptCount

  /** Returns true if the recurrence occurs at minimum minCount times. */
  def countThreshold(recurrenceCount : Long, conceptCount : Long, nrExamples : Long,
                     minCount : Long) : Boolean =
    recurrenceCount >= minCount

  /** Returns true if the recurrence has occurred at least a minimum fraction of total concept observations. */
  def conceptFraction(recurrenceCount : Long, conceptCount : Long, nrExamples : Long,
                      minFraction : Double) : Boolean =
    (recurrenceCount.toDouble / conceptCount.toDouble) >= minFraction

  /** Returns true if the recurrence has occurred at least a minimum fraction of the number of examples. */
  def totalFraction(recurrenceCount : Long, conceptCount : Long, nrExamples : Long,
                    minFraction : Double) : Boolean =
    (recurrenceCount.toDouble / nrExamples.toDouble) >= minFraction
}
