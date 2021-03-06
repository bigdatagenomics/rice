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

import java.io.File
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.MetricsContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{ TwoBitFile, ReferenceFile }
import org.bdgenomics.rice.Timers._
import org.bdgenomics.rice.algorithms.{ Index => Indexer }
import org.bdgenomics.rice.avro._
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.io.{ LocalFileByteAccess }
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Index extends BDGCommandCompanion {
  val commandName = "index"
  val commandDescription = "Build an index from a description of transcripts and a reference genome"

  def apply(cmdLine: Array[String]) = {
    new Index(Args4j[IndexArgs](cmdLine))
  }
}

class IndexArgs extends Args4jBase {
  @Argument(required = true, metaVar = "GENOME", usage = "The reference genome to index.", index = 0)
  var genome: String = null

  @Argument(required = true, metaVar = "GENES", usage = "The gene description file to use.", index = 1)
  var genes: String = null

  @Argument(required = true, metaVar = "KMER_LENGTH", usage = "The k-mer length to use for indexing.", index = 2)
  var kmerLength: Int = 0

  @Argument(required = true, metaVar = "OUTPUT", usage = "The location to write the index to.", index = 3)
  var output: String = null
}

class Index(protected val args: IndexArgs) extends BDGSparkCommand[IndexArgs] with Logging {
  val companion = Index

  def run(sc: SparkContext) {
    // load genome
    val genome = LoadingTwoBit.time {
      new TwoBitFile(new LocalFileByteAccess(new File(args.genome)))
    }

    // load gene annotations and transform to transcripts
    val transcripts = LoadingGenes.time {
      sc.loadGenes(args.genes)
        .flatMap(_.transcripts)
        .instrument()
    }

    // run indexing
    val (kmerMap, classMap) = Indexing.time {
      Indexer(genome, transcripts, args.kmerLength)
    }

    // map to avro classes and save indices
    SavingKmers.time {
      kmerMap.map(kv => {
        KmerToClass.newBuilder()
          .setKmer(kv._1)
          .setEquivalenceClass(kv._2)
          .build()
      }).adamParquetSave(args.output + "_kmers")
    }

    SavingClasses.time {
      classMap.map(kv => {
        ClassContents.newBuilder()
          .setEquivalenceClass(kv._1)
          .setKmers(kv._2.toList)
          .build()
      }).adamParquetSave(args.output + "_classes")
    }
  }
}
