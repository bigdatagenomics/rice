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

import org.apache.spark.rdd.RDD
import org.bdgenomics.RNAdam.models.{ ApproximateFusionEvent, ReadPair }
import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceRegion }
import org.bdgenomics.adam.rdd.BroadcastRegionJoin
import org.bdgenomics.formats.avro.AlignmentRecord
import scala.reflect._

object SplitAssigner {
  private[this] def referenceRegionsForEvents(events: RDD[ApproximateFusionEvent], lmin: Long, lmax: Long): RDD[(ReferenceRegion, ApproximateFusionEvent)] = {
    events.flatMap(afe => Seq((ReferenceRegion(afe.start.referenceName, afe.start.start - lmax, afe.start.end + lmax), afe),
      (ReferenceRegion(afe.end.referenceName, afe.end.start - lmax, afe.end.end + lmax), afe)))
  }

  def assignSplitsToFusions(events: RDD[ApproximateFusionEvent],
                            records: RDD[ReadPair],
                            lmin: Long,
                            lmax: Long): RDD[(ApproximateFusionEvent, ReadPair)] = {
    val referenceRegions = referenceRegionsForEvents(events, lmin, lmax)
    val flattenedRecords = records.flatMap(rp => Seq(
      if (rp.first.getReadMapped) Some((ReferenceRegion(rp.first).get, (rp.first, rp))) else None,
      if (rp.second.getReadMapped) Some((ReferenceRegion(rp.second).get, (rp.second, rp))) else None)
      .flatten)
    BroadcastRegionJoin.partitionAndJoin(events.sparkContext, referenceRegions, flattenedRecords)
      .map {
        case (afe, (_, rp)) => (afe, rp)
      }
  }
}
