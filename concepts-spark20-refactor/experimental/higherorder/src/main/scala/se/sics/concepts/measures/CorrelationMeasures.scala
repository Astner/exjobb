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

import org.apache.commons.math3.special.{Erf, Gamma}
import org.apache.commons.math3.distribution.BetaDistribution

/** Correlation functions on standard format for correlation graph derivation.
*
* Each correlation measure takes four arguments: the number of times the concepts
* have occurred together, the number of times the first- and second concept have
* occurred respectively, and the total number of examples the counts are based on.
* Each measure returns a pair of Doubles, representing the calculated correlation
* in each direction. Note that many of the correlation measures are symmetrical,
* and therefore returns the same value for both directions.
*/
object CorrelationMeasures {
  /** Returns 1 if there has been co-occurrence (pairwise count > 0), 0 otherwise. */
  def existence(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long) : (Double, Double) =
    if (pairwiseCount > 0L) (1.0D, 1.0D) else (0.0D, 0.0D)

  /** Returns the number of pairwise co-occurrences. */
  def pairwiseCount(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long) : (Double, Double) =
    (pairwiseCount.toDouble, pairwiseCount.toDouble)

  /** Returns the pairwise count weighted by individual counts.
  *
  * Returns a weighted pairwise count calculated as (nab * nab) / (na * nb), where where na is the
  * number of pairwise co-occurrences and na and nb the total individual occurrences.
  */
  def weightedCount(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long) : (Double, Double) = {
    val c = (pairwiseCount*pairwiseCount).toDouble / (countA * countB).toDouble
    (c, c)
  }

  /** Returns the overlap coefficient.
  * @see <a href="http://en.wikipedia.org/wiki/Overlap_coefficient">
  * Wikipedia description of the overlap coefficient.</a>
  */
  def overlap(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long) : (Double, Double) = {
    val c = pairwiseCount.toDouble / math.min(countA, countB).toDouble
    (c, c)
  }

  /** Returns the Soerensen-Dice coefficient.
    * @see <a href="http://en.wikipedia.org/wiki/Sorensen-Dice_coefficient">
    * Wikipedia description of the Soerensen-Dice coefficient.</a>
    */
  def sorensenDice(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long) : (Double, Double) = {
    val c = (pairwiseCount * 2).toDouble / (countA + countB).toDouble
    (c, c)
  }

  /** Returns the Jaccard coefficient.
  * @see <a href="http://en.wikipedia.org/wiki/Jaccard_index">
  * Wikipedia description of the Jaccard coefficient.</a>
  */
  def jaccard(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long) : (Double, Double) = {
    val c = pairwiseCount.toDouble / (countA + countB - pairwiseCount).toDouble
    (c, c)
  }

  /** Returns the conditional probability (for both directions). */
  def conditionalProbability(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long) :
  (Double, Double) =
    (pairwiseCount.toDouble/countA.toDouble, pairwiseCount.toDouble/countB.toDouble)

  /** Returns the expectation of the conditional probabilities under a uniform prior,
  * possibly adjusted by a specified confidence level.
  *
  * Calculates the expected value of the conditional probabilities
  * under a uniform Beta prior with selectable equivalent number of samples.
  * If a confidence level is specified, the function returns the value that
  * the fraction of the mass of the posterior beta distribution is above.
  * This results in a more conservative and stable measure,
  * returning smaller probability values when we have fewer samples, which in turn may result in more useful
  * rankings of object relations.
  * @param alpha The significance, i.e. the number of equivalent samples, of the uniform prior.
  * @param confidence The desired confidence level, or how conservative we would like our estimate to be.
  * Must be between 0 and 1. Higher levels gives a more conservative measure - suitable values are e.g.
  * 0.9 or 0.99. Values less than 0.5 lead to a more speculative measure, i.e. it will tend to estimate
  * the probability higher than the expected value.
  * @see <a href="https://en.wikipedia.org/wiki/Beta_distribution">
  * Wikipedia description of the Beta distribution.</a>
  */
  def bayesianConditionalProbability(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long,
                                     alpha: Double = 0.0, confidence: Option[Double] = None) :
  (Double, Double) = {
    val priorN = alpha / 2.0
    if(confidence.isDefined) {
      val p = 1.0 - confidence.get
      val da = pairwiseCount.toDouble + priorN
      // We add a small values to the beta parameters just to avoid running into numerical issues
      val probAB = new BetaDistribution(da + 0.00000001, countA.toDouble + priorN - pairwiseCount.toDouble + 0.00000001).inverseCumulativeProbability(p)
      val probBA = new BetaDistribution(da + 0.00000001, countB.toDouble + priorN - pairwiseCount.toDouble + 0.00000001).inverseCumulativeProbability(p)
      (probAB, probBA)
    }
    else {
      val mcountA = countA.toDouble+alpha
      val mcountB = countB.toDouble+alpha
      val probAB = if(mcountA <= 0.0) 0.0 else (pairwiseCount.toDouble+priorN) / mcountA
      val probBA = if(mcountB <= 0.0) 0.0 else (pairwiseCount.toDouble+priorN) / mcountB
      (probAB, probBA)
    }
  }

  /** Returns the pointwise mutual information.
  * @see <a href="http://en.wikipedia.org/wiki/Pointwise_mutual_information">
  * Wikipedia description of the pointwise mutual information.</a>
  */
  def pointwiseMutualInformation(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long) :
  (Double, Double) = {
    val pmi = math.log(pairwiseCount.toDouble / nrExamples.toDouble) -
              math.log(countA.toDouble / nrExamples.toDouble) -
              math.log(countB.toDouble / nrExamples.toDouble)
    (pmi, pmi)
  }

  /** Returns the expectation of the pointwise mutual information (PWMI) under a uniform prior,
  * possibly adjusted by a specified confidence level.
  *
  * Calculates the exact expected value of the pointwise mutual information
  * under a uniform Dirichlet prior with selectable equivalent number of samples.
  * If a confidence level is specified, the variance of the estimate is approximated.
  * Based on the calculated variance, the function then returns the value that
  * the fraction of the mass of the PWMI probability distribution (assumed to be normal) is above (or below for
  * negative values of the PWMI), always thresholded at zero if the quantile happens to be larger than
  * the absolute value of the expected PWMI. This results in a more conservative and stable measure,
  * returning smaller correlation values when we have fewer samples, which in turn may result in more useful
  * rankings of object relations.
  * @param alpha The significance, i.e. the number of equivalent samples, of the uniform prior.
  * @param confidence The desired confidence level, or how conservative we would like our estimate to be.
  * Must be between 0 and 1. Higher levels gives a more conservative measure - suitable values are e.g.
  * 0.9, 0.99 etc. Values less than 0.5 lead to a more speculative measure, i.e. it will tend to estimate
  * the (absolute value of the) PWMI higher than the expected value.
  * @see <a href="http://en.wikipedia.org/wiki/Pointwise_mutual_information">
  * Wikipedia description of the pointwise mutual information.</a>
  * @see <a href="https://en.wikipedia.org/wiki/Dirichlet_distribution">
  * Wikipedia description of the Dirichlet distribution.</a>
  */
  def bayesianPWMI(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long,
                   alpha: Double = 0.0, confidence: Option[Double] = None) :
  (Double, Double) = {
    val m = alpha * 0.25
    val nij = pairwiseCount.toDouble + m
    val ni = countA.toDouble + 2 * m
    val nj = countB.toDouble + 2 * m
    val n = nrExamples.toDouble + alpha

    // All counts must be larger than 0, otherwise return correlation 0.0
    if (nij <= 0.0 || ni <= 0.0 || nj <= 0.0 || n <= 0.0)
      (0.0, 0.0)
    else {
      // Exact value for expectation of pwmi
      val pwexp = Gamma.digamma(nij) -
                  Gamma.digamma(ni) -
                  Gamma.digamma(nj) +
                  Gamma.digamma(n)

      if(confidence.isDefined) {
        // Trigamma values
        val tgnij = Gamma.trigamma(nij + 0.0001)
        val tgni  = Gamma.trigamma(ni)
        val tgnj  = Gamma.trigamma(nj)
        val tgn   = Gamma.trigamma(n)

        // Exact value for variance of pwmi
        val pwvar = tgnij + tgni + tgnj - 3.0 * tgn +
                    2.0 * Math.sqrt((tgnij - tgn) * (tgni - tgn)) +
                    2.0 * Math.sqrt((tgnij - tgn) * (tgnj - tgn)) +
                    2.0 * Math.sqrt((tgni - tgn)  * (tgnj - tgn))

        val p = confidence.get

        // Assume pwmi normal distributed, calculate quantile
        val quantile = Math.sqrt(pwvar) * 1.41421356237309504880 * Erf.erfInv(2 * p - 1.0)
        if (quantile <= 0.0) (pwexp, pwexp) // Just guard against some potential numerical problems
        else {
          val pwadjusted = if (pwexp >= 0.0) Math.max(pwexp - quantile, 0.0)
                           else Math.min(pwexp + quantile, 0.0)
          (pwadjusted, pwadjusted)
        }
      }
      else (pwexp, pwexp)
    }
  }

  /** Returns the normalized pointwise mutual information.
  * @see <a href="http://en.wikipedia.org/wiki/Pointwise_mutual_information">
  * Wikipedia description of the (normalized) pointwise mutual information.</a>
  */
  def normalizedPWMI(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long) :
  (Double, Double) = {
    val lpab = math.log(pairwiseCount.toDouble / nrExamples.toDouble)
    val pmi = lpab -
              math.log(countA.toDouble / nrExamples.toDouble) -
              math.log(countB.toDouble / nrExamples.toDouble)
    val npmi = - pmi / lpab
    (npmi, npmi)
  }

  /** Returns a Bayesian version of the normalized pointwise mutual information.
  *
  * Returns the p(i,j) normalized value of the Bayesian pointwise mutual information
  * calculated in [[bayesianPWMI]].
  *
  * @see <a href="http://en.wikipedia.org/wiki/Pointwise_mutual_information">
  * Wikipedia description of the (normalized) pointwise mutual information.</a>
  */
  def normalizedBayesianPWMI(pairwiseCount : Long, countA : Long,
                             countB : Long, nrExamples : Long,
                             alpha : Double = 0.0, confidence: Option[Double] = None) :
  (Double, Double) = {
    val m = alpha * 0.25
    val nij = pairwiseCount.toDouble + m
    val n = nrExamples.toDouble + alpha
    val lpab = Math.log(Gamma.digamma(nij + 1.0) - Gamma.digamma(n + 1.0))
    val bpwmi = bayesianPWMI(pairwiseCount, countA, countB, nrExamples, alpha, confidence)
    val npmi = - bpwmi._1 / lpab
    (npmi, npmi)
  }

  /** Returns the mutual information.
  * @see <a href="http://en.wikipedia.org/wiki/Mutual_information">
  * Wikipedia description of mutual information.</a>
  */
  def mutualInformation(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long) :
  (Double, Double) = {
    val pnanb = (nrExamples - countA - countB + pairwiseCount).toDouble / nrExamples.toDouble
    val minanb = if (pnanb <= 0D) 0D
                 else { math.log(pnanb) -
                   math.log((nrExamples-countA).toDouble / nrExamples.toDouble) -
                   math.log((nrExamples-countB).toDouble / nrExamples.toDouble) }

    val panb = (countA - pairwiseCount).toDouble / nrExamples.toDouble
    val mianb = if (panb <= 0D) 0D
                else { math.log(panb) -
                  math.log(countA.toDouble / nrExamples.toDouble) -
                  math.log((nrExamples-countB).toDouble / nrExamples.toDouble) }

    val pnab = (countB - pairwiseCount).toDouble / nrExamples.toDouble
    val minab = if (pnab <= 0D) 0D
                else { math.log(pnab) -
                  math.log((nrExamples - countA).toDouble / nrExamples.toDouble) -
                  math.log(countB.toDouble / nrExamples.toDouble) }

    val pab = pairwiseCount.toDouble / nrExamples.toDouble
    val miab = if (pab <= 0D) 0D
               else { math.log(pab) -
                 math.log(countA.toDouble / nrExamples.toDouble) -
                 math.log(countB.toDouble / nrExamples.toDouble) }

    val mi = pnanb * minanb + panb * mianb + pnab * minab + pab * miab
    (mi, mi)
  }

  /** Returns the expectation of the mutual information (MI) under a uniform prior,
  * possibly adjusted by a specified confidence level.
  *
  * Calculates the exact expected value of the mutual information
  * under a uniform Dirichlet prior with selectable equivalent number of samples.
  * If a confidence level is specified, an approximation of the variance of the MI distribution
  * is also calculated (see M. Hutter, "Distribution of Mutual Information", NIPS, 2001).
  * Based on this, the function then returns the value that
  * the fraction of the mass of the PWMI probability distribution (assumed to be log-normal with
  * parameters based on the calculated expectation and variance) is above.
  * This results in a more conservative and stable measure, returning smaller values when we have fewer
  * samples.
  * @param alpha The significance, i.e. the number of equivalent samples, of the uniform prior.
  * @param confidence The desired confidence level, or how conservative we would like our estimate to be.
  * Must be between 0 and 1. Higher levels gives a more conservative measure - suitable values are e.g.
  * 0.9, 0.99 etc. Values less than 0.5 lead to a more speculative measure, i.e. it will tend to estimate
  * the MI higher than the expected value.
  * @see <a href="http://en.wikipedia.org/wiki/Mutual_information">
  * Wikipedia description of the mutual information.</a>
  * @see <a href="https://en.wikipedia.org/wiki/Dirichlet_distribution">
  * Wikipedia description of the Dirichlet distribution.</a>
  * @see <a href="http://www.hutter1.net/ai/xentropy.htm">
  * M. Hutter, "Distribution of Mutual Information".</a>
  */
  def bayesianMI(pairwiseCount : Long, countA : Long, countB : Long, nrExamples : Long,
                 alpha: Double = 0.0, confidence: Option[Double] = None) :
  (Double, Double) = {
    val m = alpha * 0.25
    val nij = pairwiseCount.toDouble + m
    val ni = countA.toDouble + 2 * m
    val nj = countB.toDouble + 2 * m
    val n = nrExamples.toDouble + alpha

    val minanb = {
      val jcount = n - ni - nj + nij
      jcount * (Gamma.digamma(jcount + 1.0) - Gamma.digamma(n - ni + 1.0) -
                Gamma.digamma(n - nj + 1.0) + Gamma.digamma(n + 1.0))
    }
    val mianb = {
      val jcount = ni - nij
      jcount * (Gamma.digamma(jcount + 1.0) - Gamma.digamma(ni + 1.0) -
                Gamma.digamma(n - nj + 1.0) + Gamma.digamma(n + 1.0))
    }
    val minab = {
      val jcount = nj - nij
      jcount * (Gamma.digamma(jcount + 1.0) - Gamma.digamma(n - ni + 1.0) -
                Gamma.digamma(nj + 1.0) + Gamma.digamma(n + 1.0))
    }
    val miab = {
      nij * (Gamma.digamma(nij + 1.0) - Gamma.digamma(ni + 1.0) -
             Gamma.digamma(nj + 1.0) + Gamma.digamma(n + 1.0))
    }

    val miexp = (1.0 / n) * (minanb + mianb + minab + miab)

    if(confidence.isDefined) {
      // Approximation of the variance of the mi
      val pnanb = (n - ni - nj + nij) / n
      val mlminanb = if (pnanb <= 0D) 0D
                     else { math.log(pnanb) -
                      math.log((nrExamples-countA).toDouble / nrExamples.toDouble) -
                      math.log((nrExamples-countB).toDouble / nrExamples.toDouble) }
      val panb = (ni - nij) / n
      val mlmianb = if (panb <= 0D) 0D
                    else { math.log(panb) -
                      math.log(countA.toDouble / nrExamples.toDouble) -
                      math.log((nrExamples-countB).toDouble / nrExamples.toDouble) }
      val pnab = (nj - nij) / n
      val mlminab = if (pnab <= 0D) 0D
                    else { math.log(pnab) -
                      math.log((nrExamples-countA).toDouble / nrExamples.toDouble) -
                      math.log(countB.toDouble / nrExamples.toDouble) }
      val pab = nij / n
      val mlmiab = if (pab <= 0D) 0D
                   else { math.log(pab) -
                     math.log(countA.toDouble / nrExamples.toDouble) -
                     math.log(countB.toDouble / nrExamples.toDouble) }

      val mlmi = pnanb * mlminanb + panb * mlmianb + pnab * mlminab + pab * mlmiab
      val sqmi = pnanb * mlminanb * mlminanb + panb * mlmianb * mlmianb +
                 pnab * mlminab * mlminab + pab * mlmiab * mlmiab

      val mivar = (1.0 / (n + 1.0)) * (sqmi - mlmi * mlmi)

      val p = 1.0 - confidence.get

      // Assume mi log-normal distributed, calculate quantile
      val scale2 = math.log(1.0 + mivar / (miexp * miexp))
      val location = math.log(miexp) - 0.5 * scale2
      val quantile = math.exp(location + math.sqrt(scale2) * 1.41421356237309504880 * Erf.erfInv(2.0 * p - 1.0))

      val miadjusted = Math.max(quantile, 0.0)
      (miadjusted, miadjusted)
    }
    else (miexp, miexp)
  }

}
