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

import org.scalatest.FunSuite
import scala.util.Random

class TranscriptGeneratorSuite extends FunSuite {

  test("transcript repetitive check should look for dupe k-mers") {
    assert(TranscriptGenerator.transcriptIsNonRepetitive(3, "ACGCCGA"))
    assert(!TranscriptGenerator.transcriptIsNonRepetitive(3, "ACGCCGACG"))
  }

  test("string length and content are correct") {
    val rv = new Random(0L)

    intercept[AssertionError] {
      TranscriptGenerator.generateString(0, rv)
    }

    val s = TranscriptGenerator.generateString(1000, rv)
    assert(s.length === 1000)
    assert(s.filter(c => c match {
      case 'A' | 'C' | 'G' | 'T' => false
      case _                     => true
    }).length === 0)
  }

  test("transcripts are independent if there are no shared k-mers") {
    assert(TranscriptGenerator.transcriptsAreIndependent(3, Seq("ACCACACCA", "TGGTGGTTTG")))
    assert(!TranscriptGenerator.transcriptsAreIndependent(3, Seq("ACCACACCA", "ACCTGGTGGTTTG")))
  }

  test("transcript generator should generate independent, correct length transcripts") {
    val tLen = Array(1000, 500, 800)

    val (t, names, tMap, cMap) = TranscriptGenerator.generateIndependentTranscripts(20,
      tLen,
      Some(1L))

    assert(t.length === 3)

    // second way to evaluate independence: intersection vs. other transcripts is empty
    (0 until tLen.length).foreach(i => {
      assert(t(i).length === tLen(i))
      val tMers = t(i).sliding(20).toSet
      val tName = names(i)
      assert(i.toString == tName)
      tMers.foreach(tmer => assert(tMap(tmer) === i.toLong))
      assert(cMap(i.toLong).size === 1)
      assert(cMap(i.toLong).head === i.toString)
      ((i + 1) until tLen.length).foreach(j => {
        assert(tMers.intersect(t(j).sliding(20).toSet).size === 0)
      })
    })
  }
}
