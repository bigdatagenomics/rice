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

import org.bdgenomics.RNAdam.models.{ ReadPair, ApproximateFusionEvent }
import org.bdgenomics.adam.models.{ SequenceRecord, SequenceDictionary, ReferenceRegion }
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.{ Contig, AlignmentRecord }

class SplitAssignerSuite extends SparkFunSuite {
  sparkTest("Split Assigner makes correct assignment given direct query") {
    val t1t2afe = ApproximateFusionEvent(ReferenceRegion("t1", 100, 115), ReferenceRegion("t2", 100, 115))
    val t1Record = AlignmentRecord.newBuilder()
      .setContig(Contig.newBuilder().setContigName("t1").build())
      .setStart(110)
      .setEnd(115)
      .setCigar("5M")
      .setReadMapped(true)
      .build()
    val unmappedRecord = AlignmentRecord.newBuilder()
      .setReadMapped(false)
      .build()
    val readPair = ReadPair(t1Record, unmappedRecord)
    val fusions = sc.parallelize(Seq(t1t2afe))
    val records = sc.parallelize(Seq(readPair))
    val seqDict = SequenceDictionary(SequenceRecord("t1", 200), SequenceRecord("t2", 200))
    val assignments = SplitAssigner.assignSplitsToFusions(fusions, records, seqDict, 1, 5)
    assert(assignments.count() === 1)
    assert(assignments.first() === (t1t2afe, readPair))
  }

  sparkTest("Split Assigner finds multiple split reads") {
    val t1t2afe = ApproximateFusionEvent(ReferenceRegion("t1", 100, 115), ReferenceRegion("t2", 100, 115))
    val t1Record = AlignmentRecord.newBuilder()
      .setContig(Contig.newBuilder().setContigName("t1").build())
      .setStart(110)
      .setEnd(115)
      .setCigar("5M")
      .setReadMapped(true)
      .build()
    val t2Record = AlignmentRecord.newBuilder()
      .setContig(Contig.newBuilder().setContigName("t2").build())
      .setStart(105)
      .setEnd(110)
      .setCigar("5M")
      .setReadMapped(true)
      .build()
    val unmappedRecord = AlignmentRecord.newBuilder()
      .setReadMapped(false)
      .build()
    val readPairT1 = ReadPair(t1Record, unmappedRecord)
    val readPairT2 = ReadPair(t2Record, unmappedRecord)
    val fusions = sc.parallelize(Seq(t1t2afe))
    val records = sc.parallelize(Seq(readPairT1, readPairT2))
    val seqDict = SequenceDictionary(SequenceRecord("t1", 200), SequenceRecord("t2", 200))
    val assignments = SplitAssigner.assignSplitsToFusions(fusions, records, seqDict, 1, 5)
    assert(assignments.count() === 2)
    assert(assignments.collect().toSet === Set((t1t2afe, readPairT1), (t1t2afe, readPairT2)))
  }
}
