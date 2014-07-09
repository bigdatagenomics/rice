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
package org.bdgenomics.RNAdam.algorithms.defuse

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.bdgenomics.RNAdam.models.{ ApproximateFusionEvent, FusionEvent, ReadPair }
import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.RNAdam.models.{ ApproximateFusionEvent, FusionEvent }

object Defuse {
  def run(records: RDD[ADAMRecord],
    alpha: Double): RDD[FusionEvent] = {
    val (concordant, spanning, split) = classify(records)
    val (lmin, lmax) = findPercentiles(concordant, alpha)
    val graph = buildGraph(spanning, lmax)
    val fusions = bestFusions(graph)
    val splitRecordToFusion = assignSplitsToFusions(fusions, split, lmin, lmax)
    val exactBoundary = findExactBoundaryForFusions(splitRecordToFusion)
    trueFusions(graph, exactBoundary)
  }

  /**
   * Anita and Dennis are working on this...
   * @param records
   * @return
   */
  def classify(records: RDD[ADAMRecord]): (RDD[ReadPair], RDD[ReadPair], RDD[ReadPair]) =
    ???

  /**
   * Calculates a fragment length distribution, and excludes outliers given an
   * alpha parameter.
   *
   * @param concordantRecords An RDD of ADAM reads.
   * @param alpha The top/bottom % of reads to exclude.
   * @return (l_{min}, l_{max}): Return the min and max length.
   */
  def findPercentiles(concordantRecords: RDD[ReadPair], alpha: Double): (Long, Long) =
    FragmentLengthDistribution.findPercentiles(concordantRecords, alpha)

  /**
   * Frank is working on this...
   * @param spanningRecords
   * @param lmax
   * @return
   */
  def buildGraph(spanningRecords: RDD[ReadPair], lmax: Long): Graph[ReadPair, ApproximateFusionEvent] =
    ???

  /**
   * Timothy is working on this...
   *
   * @param graph
   * @return
   */
  def bestFusions(graph: Graph[ReadPair, ApproximateFusionEvent]): RDD[ApproximateFusionEvent] =
    ???

  /**
   * Carl is working on this...
   *
   * @param fusions
   * @param splitRecords
   * @param lmin
   * @param lmax
   * @return
   */
  def assignSplitsToFusions(fusions: RDD[ApproximateFusionEvent], splitRecords: RDD[ReadPair], lmin: Long, lmax: Long): RDD[(ApproximateFusionEvent, ReadPair)] =
    ???

  def findExactBoundaryForFusions(splitRecordToFusions: RDD[(ApproximateFusionEvent, ReadPair)]): RDD[(ApproximateFusionEvent, FusionEvent)] =
    ???

  def trueFusions(graph: Graph[ReadPair, ApproximateFusionEvent], exactFusions: RDD[(ApproximateFusionEvent, FusionEvent)]): RDD[FusionEvent] =
    ???
}
