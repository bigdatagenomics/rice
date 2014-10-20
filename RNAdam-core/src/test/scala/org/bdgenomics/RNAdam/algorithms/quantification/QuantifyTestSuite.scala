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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.util.SparkFunSuite

class QuantifyTestSuite extends SparkFunSuite {

  sparkTest("test of mapKmersToClasses") {
    val kmerToEquivalenceClass: RDD[(String, Long)] = sc.parallelize(Seq(("a", 2),
      ("b", 3),
      ("c", 2),
      ("d", 1),
      ("e", 3)))
    val kmerCounts: RDD[(String, Long)] = sc.parallelize(Seq(("d", 80), ("a", 25), ("c", 35), ("b", 37), ("e", 38)))
    val classCounts: RDD[(Long, Long)] = Quantify.mapKmersToClasses(kmerCounts, kmerToEquivalenceClass)
    assert(classCounts.count() === 3)
    assert(classCounts.filter((x: (Long, Long)) => x._1 == 1).first() === (1, 80))
    assert(classCounts.filter((x: (Long, Long)) => x._1 == 2).first() === (2, 60))
    assert(classCounts.filter((x: (Long, Long)) => x._1 == 3).first() === (3, 75))
  }

}
