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

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.NucleotideContigFragment
import org.bdgenomics.adam.models.{ Exon, Transcript }

object Index extends Serializable with Logging {

  /**
   * Computes an index, given a reference sequence, a set of transcripts, and a k-mer
   * length. An index provides bidirectional mappings between k-mers and equivalence classes.
   *
   * This code is based on the implementation of Sailfish, which is described in:
   *
   * Patro, Rob, Stephen M. Mount, and Carl Kingsford. "Sailfish: alignment-free isoform
   * quantification from RNA-seq reads using lightweight algorithms." arXiv preprint arXiv:1308.3700 (2013).
   *
   * and:
   *
   * Patro, Rob, Stephen M. Mount, and Carl Kingsford. "Sailfish enables alignment-free
   * isoform quantification from RNA-seq reads using lightweight algorithms." Nature biotechnology 32.5 (2014): 462-464.
   *
   * @param reference An RDD containing fragments which contain reference contigs.
   * @param transcripts An RDD containing transcripts.
   * @param kmerLength The length _k_ of the k-mers for us to index.
   * @return Returns a tuple containing two RDDs: the first RDD maps k-mers to equivalence class
   *         IDs, the second maps equivalence class IDs to an iterable of member k-mers.
   */
  def apply(reference: RDD[NucleotideContigFragment],
            transcripts: RDD[Transcript],
            kmerLength: Int): (RDD[(String, Long)], RDD[(Long, Iterable[String])]) = {

    // cut exons out of the reference sequence
    val exonSequence = cutOutExons(reference, transcripts)

    // given all the transcripts, find the equivalence classes
    val (equivalenceClasses,
      exonToClassMap) = findEquivalenceClasses(transcripts)

    // cut all equivalence classes into kmers
    val classKmers = cutClassesIntoKmers(exonSequence,
      exonToClassMap,
      kmerLength)

    // reverse mapping (i.e., get kmer to class mapping)
    val kmersToClasses = reverseClassMapping(classKmers)

    (kmersToClasses, classKmers)
  }

  /**
   * Given a set of transcripts and reference contigs, this method cuts out the
   * nucleotide strings that correspond to exons. It then returns an RDD of tuples
   * containing exon descriptors and nucleotide strings.
   *
   * @param contigs An RDD of reference contigs.
   * @param transcripts An RDD of transcripts.
   * @return Returns an RDD of exon ID/nucleotide string tuples.
   */
  private[quantification] def cutOutExons(contigs: RDD[NucleotideContigFragment],
                                          transcripts: RDD[Transcript]): RDD[(String, String)] = {
    ???
  }

  /**
   * Given an RDD of transcripts, this method finds the k-mer equivalence classes. K-mer
   * equivalence classes represent k-mers that show up with equal abundance across several
   * transcripts---in our method, we simplify by just looking for exons that occur across
   * several transcripts. This method returns two RDDs: one maps equivalence class IDs to
   * a list of transcripts, and the other maps exon IDs to equivalence class IDs.
   *
   * @param transcripts An RDD of transcripts.
   * @return Returns two RDDs: one maps equivalence class IDs to a list of transcripts,
   *         and the other maps exon IDs to equivalence class IDs.
   */
  private[quantification] def findEquivalenceClasses(transcripts: RDD[Transcript]): (RDD[(Long, Iterable[String])], RDD[(String, Long)]) = {
    ???
  }

  /**
   * Given a list of exon IDs and the sequences of those exons, a mapping between exon IDs and
   * equivalence classes, and a k-mer length, this method returns all k-mers which belong to
   * all equivalence classes.
   *
   * @param exonSequence An RDD containing tuples with exon IDs and exon sequences.
   * @param exonToClassMap An RDD containing tuples that map exon IDs to equivalence classes.
   * @param kmerLength The length _k_ to use for our k-mers.
   * @return Returns an RDD of tuples where each tuple represents an equivalence class and
   *         its k-mers.
   *
   * @see reverseClassMapping
   */
  private[quantification] def cutClassesIntoKmers(exonSequence: RDD[(String, String)],
                                                  exonToClassMap: RDD[(String, Long)],
                                                  kmerLength: Int): RDD[(Long, Iterable[String])] = {
    ???
  }

  /**
   * This method reverses the k-mer class mapping given by the k-mer generation process. Specifically, it
   * takes an RDD which contains the equivalence class ID and an iterator across all k-mers in that
   * equivalence class, and returns an RDD which maps k-mer strings to equivalence class IDs.
   *
   * @param classesAndKmers An RDD of tuples where each tuple maps an equivalence class ID to an
   *                        iterable which contains the k-mers that belong to the equivalence class.
   * @return An RDD which maps k-mer strings to equivalence class IDs.
   *
   * @see cutClassesIntoKmers
   */
  private[quantification] def reverseClassMapping(classesAndKmers: RDD[(Long, Iterable[String])]): RDD[(String, Long)] = {
    ???
  }
}
