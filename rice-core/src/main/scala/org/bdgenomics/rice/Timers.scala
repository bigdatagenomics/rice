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
package org.bdgenomics.rice

import org.bdgenomics.utils.instrumentation.Metrics

/**
 * Contains [[Timers]] that are used to instrument rice.
 */
private[rice] object Timers extends Metrics {

  // CLI
  val LoadingTwoBit = timer("Loading Two Bit File")
  val LoadingGenes = timer("Loading gene descriptions")
  val Indexing = timer("Indexing k-mers")
  val SavingKmers = timer("Saving index - kmer map")
  val SavingClasses = timer("Saving index - class map")

  // Indexing
  val Broadcast = timer("Broadcasting Reference File")
  val Extract = timer("Extracting Transcript from Reference")
  val SplitKmers = timer("Splitting k-mers from Transcript")
  val GenerateClasses = timer("Generating Equivalence Classes")
  val GenerateIndices = timer("Mapping Equivalence Classes to Indices")

  // Additional Indexing Timers
  val KmersAndTranscript = timer("Sorting Kmers By Transcript")
  val CollectingKmersByCount = timer("Collecting kmers by count (ReduceByKey)")
  val SortByTranscript = timer("Sorting kmers by transcript (Map)")
  val CollectingKmersByTranscript = timer("Collecting kmers by transcript (GroupByKey)")
  val DistillingEqClasses = timer("Mapping collected groups into equivalence classes")
  val NumberingEqClasses = timer("Numbering equivalence classes")

  // Quantification
  val ExtractTranscriptLengths = timer("Extraction Transcript Lengths")
  val CountKmers = timer("Counting k-mers")
  val TareKmers = timer("Calibrate k-mer counts vs. GC Content")
  val CountEquivalenceClasses = timer("Map k-mers to Equivalence Classes")
  val NormalizingCounts = timer("Normalizing Input Counts")
  val InitializingEM = timer("Initializing EM Algorithm")
  val InitializingCounts = timer("Initializing Counts")
  val InitializingMu = timer("Initializing µ's")
  val RunningEMIter = timer("Running an Iteration of EM")
  val EStage = timer("E stage of EM")
  val MStage = timer("M stage of EM")
  val CalibratingForLength = timer("Calibrating vs. Transcript Length")
  val JoiningAgainstTranscripts = timer("Joining vs. Transcripts")
}
