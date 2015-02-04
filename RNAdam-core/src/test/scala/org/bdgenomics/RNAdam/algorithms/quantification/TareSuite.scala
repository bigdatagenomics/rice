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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.RNAdam.utils.TranscriptGenerator
import scala.math.{ abs, exp, log, max, min }
import scala.util.Random

class TareSuite extends SparkFunSuite {

  def fpEquals(a: Double, b: Double, eps: Double = 1e-6): Boolean = {
    abs(a - b) <= eps
  }

  test("can't process illegal k-mers") {
    intercept[AssertionError] {
      Tare.kmerToDinucFeature("AN", 1L)
    }
    intercept[AssertionError] {
      Tare.kmerToDinucFeature("A", 10L)
    }
    intercept[AssertionError] {
      Tare.kmerToDinucFeature("ANTNC", 100L)
    }
  }

  test("chop a 2-mer into a feature") {
    val featureAA = Tare.kmerToDinucFeature("AA", 1000L)
    assert(fpEquals(featureAA.label, log(1000L)))
    assert(fpEquals(featureAA.features(0), 1.0))
    (1 to 15).foreach(i => assert(fpEquals(featureAA.features(i), 0.0)))

    val featureTT = Tare.kmerToDinucFeature("TT", 100L)
    assert(fpEquals(featureTT.label, log(100L)))
    assert(fpEquals(featureTT.features(15), 1.0))
    (0 to 14).foreach(i => assert(fpEquals(featureTT.features(i), 0.0)))
  }

  test("chop an 5-mer with a bad base into a feature") {
    val feature = Tare.kmerToDinucFeature("AANTT", 4321L)
    assert(fpEquals(feature.label, log(4321L)))
    assert(fpEquals(feature.features(0), 0.5))
    assert(fpEquals(feature.features(15), 0.5))
    (1 to 14).foreach(i => assert(fpEquals(feature.features(i), 0.0)))
  }

  sparkTest("generate biased kmers and try correcting their counts") {
    val sampleString = TranscriptGenerator.generateString(500, new Random(121212L))

    // generate kmers corresponding to log space gc bias curve of 2.0 + 1.0 (gc - 0.5)
    val kmerSamples = sampleString.sliding(15)
      .map(s => {
        // compute bias and use curve for count
        val gc = s.count(c => c == 'C' || c == 'G').toDouble / 15.0
        val count = (100.0 * exp(2.0 + 1.0 * (gc - 0.5))).toLong

        (s, count)
      }).toSeq

    // build rdd and fit sequence context curve
    val rdd = sc.parallelize(kmerSamples)
    val originalMax = rdd.map(kv => kv._2).reduce(_ max _)
    val originalMin = rdd.map(kv => kv._2).reduce(_ min _)
    val calibratedRdd = Tare.calibrateKmers(rdd)
      .cache()

    // calibration should reduce max and increase min
    val newMax = calibratedRdd.map(kv => kv._2).reduce(_ max _)
    val newMin = calibratedRdd.map(kv => kv._2).reduce(_ min _)

    assert(originalMax > newMax)
    assert(originalMin < newMin)
  }
}
