/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.RNAdam.algorithms.quantification

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{ LabeledPoint, LinearRegressionWithSGD }
import scala.math.{ exp, log }

object Tare extends Serializable {

  /**
   * Converts a base into an integer number. Only works for A/C/G/T, both
   * lower and upper case. Will throw an exception for other bases, user is
   * expected to check before passing to function.
   *
   * @param base The base to convert into an integer.
   * @return Returns a mapping from a base to an integer.
   */
  private def idx(base: Char): Int = base match {
    case 'A' | 'a' => 0
    case 'C' | 'c' => 1
    case 'G' | 'g' => 2
    case 'T' | 't' => 3
  }

  /**
   * @param base Base to examine.
   * @return Returns true if the base is a "valid" base as defined by the idx function.
   *
   * @see idx
   */
  private def validBase(base: Char): Boolean = base match {
    case 'A' | 'a' | 'C' | 'c' | 'G' | 'g' | 'T' | 't' => true
    case _ => false
  }

  /**
   * Hashes a dinucleotide sequence context into an integer.
   *
   * @param context Sequence context to hash.
   * @return Mapping from sequence context into an int; scale is 0-15.
   */
  private def dinucToIdx(context: String): Int = {
    4 * idx(context(0)) + idx(context(1))
  }

  /**
   * @param context Dinucleotide sequence context to evaluate.
   * @return Returns true if the context is valid. Validity is defined by the
   *         idx function.
   *
   * @see idx
   */
  private def isValidContext(context: String): Boolean = {
    context.length == 2 &&
      validBase(context(0)) &&
      validBase(context(1))
  }

  /**
   * Featurizes a k-mer. Cuts the k-mer into dinucleotide sequence contexts, filters out invalid
   * sequences, and then builds a feature vector that maps the log of the multiplicity to the
   * fraction of each context occurrence.
   *
   * @param kmer K-mer string to featurize.
   * @param multiplicity Observed k-mer count.
   * @return Vector mapping k-mer to log of multiplicity and sequence context counts.
   */
  private[quantification] def kmerToDinucFeature(kmer: String, multiplicity: Long): LabeledPoint = {
    // slice kmers into contexts
    val contexts = kmer.sliding(2).filter(isValidContext).toIterable
    assert(contexts.size > 0, "k-mer: " + kmer + " does not contain any valid contexts.")
    val ctxReciprocal = 1.0 / contexts.size.toDouble

    // populate initial context array
    val contextCounts = Array.fill[Double](16) { 0.0 }

    // update counts
    contexts.foreach(c => contextCounts(dinucToIdx(c)) += ctxReciprocal)

    new LabeledPoint(log(multiplicity.toDouble), Vectors.dense(contextCounts))
  }

  /**
   * Translates k-mers and counts into features and then trains a linear regression model.
   * After the model is trained, the k-mer count is recalibrated.
   *
   * @param kmers An RDD of k-mer strings and counts.
   * @return Returns a calibrated RDD of k-mers and counts.
   */
  def calibrateKmers(kmers: RDD[(String, Long)]): RDD[(String, Long)] = {
    // featurize kmers
    val countAccumulator = kmers.context.accumulator(0L)
    val multiplicityAccumulator = kmers.context.accumulator(0L)
    val kmersAndFeatures = kmers.map(kv => {
      countAccumulator += 1
      multiplicityAccumulator += kv._2
      (kv._1, kmerToDinucFeature(kv._1, kv._2))
    }).cache()

    // train linear model
    val model = new LinearRegressionWithSGD().setIntercept(true).run(kmersAndFeatures.map(kv => kv._2))
    val bcastModel = kmersAndFeatures.context.broadcast(model)

    // compute sample mean
    val mean = log(multiplicityAccumulator.value.toDouble / countAccumulator.value.toDouble)

    // transform back 
    val calibratedKmers = kmersAndFeatures.map(kv => {
      (kv._1, exp(mean + (kv._2.label - bcastModel.value.predict(kv._2.features))).toLong)
    })

    // unpersist feature rdd
    kmersAndFeatures.unpersist()

    calibratedKmers
  }
}
