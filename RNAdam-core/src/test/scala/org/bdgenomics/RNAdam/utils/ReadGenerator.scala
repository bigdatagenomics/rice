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
package org.bdgenomics.RNAdam.utils

import org.bdgenomics.formats.avro.AlignmentRecord
import scala.math.abs
import scala.util.Random

object ReadGenerator {

  /**
   * Generates a set of read sequences from a transcript string. Only generates
   * the read sequence, no names or quality scores or the like. Performs ideal
   * sampling (no GC bias, no errors).
   *
   * @param transcript String to sample reads from.
   * @param reads The number of reads to generate.
   * @param rv The random variable to use when generating reads.
   * @param readLength The length of reads to generate.
   * @return Returns a sequence of reads.
   */
  private[utils] def generateReads(transcript: String,
                                   reads: Int,
                                   rv: Random,
                                   readLength: Int): Seq[AlignmentRecord] = {
    val transcriptLength = transcript.length
    (0 until reads).map(i => {
      val readStart = rv.nextInt(transcriptLength - readLength)
      AlignmentRecord.newBuilder()
        .setSequence(transcript.substring(readStart, readStart + readLength))
        .build()
    })
  }

  /**
   * Generates a set of reads, given a set of transcripts with given relative abundances.
   *
   * @param transcripts Transcript sequences to generate reads from.
   * @param relativeAbundances Relative abundance of each transcript. Must sum to 1.
   * @param numReads The number of reads to generate.
   * @param readLength The length of reads to generate.
   * @param seed An optional seed for the sampling process.
   * @return Returns a seq of reads.
   */
  def apply(transcripts: Seq[String],
            relativeAbundances: Seq[Double],
            numReads: Int,
            readLength: Int,
            seed: Option[Long]): Seq[AlignmentRecord] = {
    val lengths = transcripts.map(_.length)
    assert(lengths.min >= readLength, "Transcripts must be longer than read length.")
    assert(relativeAbundances.length == transcripts.length)
    assert(abs(relativeAbundances.sum - 1.0) < 0.001)

    // calculate fraction of reads from each transcript
    val preWeights = (0 until relativeAbundances.length).map(i => lengths(i) * relativeAbundances(i))
    val totalWeight = preWeights.sum
    val fraction = preWeights.map(_ / totalWeight)

    // build random variable
    val rv = seed.fold(new Random)(new Random(_))

    // compute read assignments
    val avgLength = lengths.sum.toDouble / lengths.length.toDouble
    (0 until relativeAbundances.length).flatMap(i => {
      val reads = (fraction(i) * numReads.toDouble).toInt
      generateReads(transcripts(i), reads, rv, readLength)
    })
  }
}
