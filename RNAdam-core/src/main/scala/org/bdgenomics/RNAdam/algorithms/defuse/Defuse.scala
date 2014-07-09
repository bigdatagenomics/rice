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
import org.bdgenomics.RNAdam.models.FusionEvent
import org.bdgenomics.formats.avro.ADAMRecord

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

  def classify(records: RDD[ADAMRecord]): (RDD[ADAMRecord], RDD[ADAMRecord], RDD[ADAMRecord]) =
    ???

  def findPercentiles(concordantRecords: RDD[ADAMRecord], alpha: Double): (Long, Long) =
    ???

  def buildGraph(spanningRecords: RDD[ADAMRecord], lmax: Long): Graph[ADAMRecord, ApproximateFusionEvent] =
    ???

  def bestFusions(graph: Graph[ADAMRecord, ApproximateFusionEvent]): RDD[ApproximateFusionEvent] =
    ???

  def assignSplitsToFusions(fusions: RDD[ApproximateFusionEvent], splitRecords: RDD[ADAMRecord], lmin: Long, lmax: Long): RDD[(ApproximateFusionEvent, ADAMRecord)] =
    ???

  def findExactBoundaryForFusions(splitRecordToFusions: RDD[(ApproximateFusionEvent, ADAMRecord)]): RDD[(FusionEvent, FusionEvent)] =
    ???

  def trueFusions(graph: Graph[ADAMRecord, ApproximateFusionEvent], exactFusions: RDD[(ApproximateFusionEvent, FusionEvent)]): RDD[FusionEvent] =
    ???
}
