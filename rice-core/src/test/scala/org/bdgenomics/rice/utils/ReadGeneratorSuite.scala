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
package org.bdgenomics.rice.utils

import org.scalatest.FunSuite
import scala.util.Random

class ReadGeneratorSuite extends FunSuite {

  test("generate 100 reads, all 4 bp long") {
    val reads = ReadGenerator.generateReads("ACACA", 100, new Random(1L), 4)
    reads.foreach(r => {
      assert(r.getSequence.length === 4)
      assert(r.getSequence == "ACAC" || r.getSequence == "CACA")
    })
  }

  test("generate reads from independent transcripts") {
    val (transcripts, _, _, _) = TranscriptGenerator.generateIndependentTranscripts(20,
      Seq(100, 50, 25),
      Some(2L))

    val reads = ReadGenerator(transcripts,
      Seq(0.3333, 0.3333, 0.3333),
      175,
      20,
      Some(3L))

    assert(reads.length === 175)

    val transcriptSlices = transcripts.map(_.sliding(20).toSet)
    val counts = Array(0, 0, 0)

    reads.foreach(r => {
      val seq = r.getSequence
      if (transcriptSlices(0)(seq)) {
        counts(0) += 1
      } else if (transcriptSlices(1)(seq)) {
        counts(1) += 1
      } else if (transcriptSlices(2)(seq)) {
        counts(2) += 1
      } else {
        assert(false)
      }
    })

    assert(counts(0) === 100)
    assert(counts(1) === 50)
    assert(counts(2) === 25)
  }
}
