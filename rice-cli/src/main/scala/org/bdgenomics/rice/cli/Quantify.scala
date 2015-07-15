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
package org.bdgenomics.rice.cli

import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.rice.algorithms.{ Quantify => Quantifier }
import org.bdgenomics.rice.avro._
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.HadoopUtil
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.util.ContextUtil

object Quantify extends BDGCommandCompanion {
  val commandName = "quantify"
  val commandDescription = "Estimate isoform abundances from RNA-seq data."

  def apply(cmdLine: Array[String]) = {
    new Quantify(Args4j[QuantifyArgs](cmdLine))
  }
}

class QuantifyArgs extends Args4jBase {
  @Argument(required = true, metaVar = "READS", usage = "The reads to quantify.", index = 0)
  var reads: String = null

  @Argument(required = true, metaVar = "INDEX", usage = "The index to use.", index = 1)
  var index: String = null

  @Argument(required = true, metaVar = "GENES", usage = "The gene description file to use.", index = 2)
  var genes: String = null

  @Argument(required = true, metaVar = "KMER_LENGTH", usage = "The k-mer length to use for quantification. Must be the same as the k-mer length used for indexing.", index = 3)
  var kmerLength: Int = 0

  @Argument(required = true, metaVar = "OUTPUT", usage = "The location to write the quantification results to.", index = 4)
  var output: String = null

  @Args4jOption(required = false, name = "-max_iterations", usage = "The maximum number of iterations of EM to run. Default is 50.")
  var maxIterations: Int = 50

  @Args4jOption(required = false, name = "-disable_kmer_calibration", usage = "Disables k-mer abundance calibration.")
  var disableKmerCalibration = false

  @Args4jOption(required = false, name = "-disable_length_calibration", usage = "Disables transcript length calibration.")
  var disableLengthCalibration = false
}

class Quantify(protected val args: QuantifyArgs) extends BDGSparkCommand[QuantifyArgs] with Logging {
  val companion = Quantify

  def run(sc: SparkContext) {

    // load reads
    val reads = sc.loadAlignments(args.reads)

    // load index maps
    val job = HadoopUtil.newJob(sc)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[KmerToClass]])
    val kmerMap: RDD[(String, Long)] = sc.newAPIHadoopFile(args.index + "_kmers",
      classOf[ParquetInputFormat[KmerToClass]],
      classOf[Void],
      classOf[KmerToClass],
      ContextUtil.getConfiguration(job))
      .map(kv => (kv._2.getKmer, kv._2.getEquivalenceClass))
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[ClassContents]])
    val classMap: RDD[(Long, Iterable[String])] = sc.newAPIHadoopFile(args.index + "_classes",
      classOf[ParquetInputFormat[ClassContents]],
      classOf[Void],
      classOf[ClassContents],
      ContextUtil.getConfiguration(job))
      .map(kv => (kv._2.getEquivalenceClass, kv._2.getKmers.toIterable))

    // load transcripts
    val transcripts = sc.loadGenes(args.genes)
      .flatMap(_.transcripts)

    // run quantification
    val abundances = Quantifier(reads,
      kmerMap,
      classMap,
      transcripts,
      args.kmerLength,
      args.maxIterations,
      !args.disableKmerCalibration,
      !args.disableLengthCalibration)

    // save to text file
    abundances.map(kv => kv._1.id + ", " + kv._2)
      .saveAsTextFile(args.output)
  }
}
