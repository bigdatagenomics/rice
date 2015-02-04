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

import scala.util.Random

object TranscriptGenerator {

  /**
   * Checks to see if a string is non-repetitive. A string is considered repetitive
   * if it contains any k-mers that have multiplicity greater than or equal to one.
   *
   * @param k The k-mer length.
   * @param s The string to check.
   * @return True if the string is unique.
   */
  private[utils] def transcriptIsNonRepetitive(k: Int, s: String): Boolean = {
    val kmers = s.sliding(k).toSeq
    kmers.size == kmers.toSet.size
  }

  /**
   * @param length The string length.
   * @param rv A random variable to use.
   * @return Returns a random string composed of 'A', 'C', 'G', 'T'.
   */
  def generateString(length: Int, rv: Random): String = {
    assert(length > 0)
    val sb = new StringBuilder(length)
    (0 until length).foreach(i => {
      sb.append(rv.nextInt(3) match {
        case 0 => 'A'
        case 1 => 'C'
        case 2 => 'G'
        case _ => 'T'
      })
    })

    sb.toString()
  }

  /**
   * Checks to see if all strings are independent. We define two strings as independent
   * if there are no k-mers that show up in both strings.
   *
   * @param k The k-mer length.
   * @param t The seq of transcripts to check.
   * @return True if all transcripts are independent.
   */
  private[utils] def transcriptsAreIndependent(k: Int, t: Seq[String]): Boolean = {
    val tMers = t.map(_.sliding(k).toSet)

    // if transcripts are independent, the union of all sets is equal in size to the
    // sum of the sizes of all sets
    val totalSet = tMers.reduce(_ ++ _)
    val sumSize = tMers.map(_.size).reduce(_ + _)

    totalSet.size == sumSize
  }

  /**
   * Generates more realistic transcripts. Does this by generating independent strings
   * per equivalence class, which are then molded into transcripts.
   *
   * @param kmerLength K-mer length to use.
   * @param classSize The size of each equivalence class.
   * @param classMultiplicity The multiplicity of k-mers per equivalence class.
   * @param classMembership Per transcript, the classes that make up this transcript.
   * @param randomSeed An optional random seed.
   * @return Returns a tuple of (Transcript sequences, transcript names, map from k-mers
   *         to equivalence class, map from equivalence classes to transcripts).
   */
  def generateTranscripts(kmerLength: Int,
                          classSize: Seq[Int],
                          classMultiplicity: Seq[Int],
                          classMembership: Seq[Set[Int]],
                          randomSeed: Option[Long] = None): (Seq[String], Seq[String], Map[String, Long], Map[Long, Iterable[String]]) = {
    assert(classMembership.length == classMembership.toSet.size, "Class membership composition must not be repeated.")

    // generate random instance
    val rv = randomSeed.fold(new Random())(new Random(_))

    // generate independent sequences
    val (sequences, _, kmerMap, _) = generateIndependentTranscripts(kmerLength, classSize, randomSeed)

    // generate transcripts by randomly merging strings from equivalence sets
    val transcripts = classMembership.map(s => {
      // randomly order string components
      val components = rv.shuffle(s.toSeq.flatMap(i => {
        (0 until classMultiplicity(i)).map(j => i)
      }))

      // get transcript strings and reduce down
      components.map(i => sequences(i)).reduce(_ + _)
    })

    // generate transcript names
    val names = (0 until classMembership.length).map(i => i.toString)

    // generate map from equivalence classes to transcripts
    val tMap = classMembership.zipWithIndex
      .flatMap(vk => vk._1.map(i => (i, vk._2)))
      .groupBy(kv => kv._1)
      .map(kv => (kv._1.toLong, kv._2.map(p => p._2.toString).toIterable))

    (transcripts, names, kmerMap, tMap)
  }

  /**
   * Generates independent and non-repetitive transcripts.
   *
   * @param The k-mer length to use when verifying independence and non-repetitiveness.
   * @param The lengths of transcripts to generate.
   * @param An optional seed for randomization.
   * @return Returns a tuple of (Transcript sequences, transcript names, map from k-mers
   *         to equivalence class, map from equivalence classes to transcripts).
   */
  def generateIndependentTranscripts(kmerLength: Int,
                                     transcriptLengths: Seq[Int],
                                     randomSeed: Option[Long] = None): (Seq[String], Seq[String], Map[String, Long], Map[Long, Iterable[String]]) = {
    assert(kmerLength > 0, "k-mer length must be greater than 0. Was provided: " + kmerLength)
    assert(transcriptLengths.forall(_ > 0), "Transcript lengths must be greater than 0. Was provided: " + kmerLength)

    // generate us a random variable
    val rv = randomSeed.fold(new Random)(new Random(_))

    var transcripts = Seq[String]()

    do {
      transcripts = transcriptLengths.map(generateString(_, rv))
    } while (!transcriptsAreIndependent(kmerLength, transcripts) &&
      transcripts.forall(transcriptIsNonRepetitive(kmerLength, _)))

    val tNames = (0 until transcripts.length).map(_.toString)
    val classMap = (0 until transcripts.length).map(tId => (tId.toLong, Iterable(tId.toString))).toMap
    val tMerMap = (0 until transcripts.length).flatMap(tId => {
      transcripts(tId).sliding(kmerLength).map(k => (k, tId.toLong))
    }).toMap

    (transcripts, tNames, tMerMap, classMap)
  }

}
