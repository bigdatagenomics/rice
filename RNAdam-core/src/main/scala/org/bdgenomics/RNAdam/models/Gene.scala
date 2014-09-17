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
package org.bdgenomics.RNAdam.models

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Strand, Feature }
import scala.collection.JavaConversions._

/**
 * A 'gene model' is a small, hierarchical collection of objects: Genes, Transcripts, and Exons.
 * Each Gene contains a collection of Transcripts, and each Transcript contains a collection of
 * Exons, and together they describe how the genome is transcribed and translated into a family
 * of related proteins (or other RNA products that aren't translated at all).
 *
 * This review,
 * Gerstein et al. "What is a gene, post-ENCODE? History and updated definition" Genome Research (2007)
 * http://genome.cshlp.org/content/17/6/669.full
 *
 * is a reasonably good overview both of what the term 'gene' has meant in the past as well as
 * where it might be headed in the future.
 *
 * Here, we aren't trying to answer any of these questions about "what is a gene," but rather to
 * provide the routines necessary to _re-assemble_ hierarchical models of genes that have been
 * flattened into features (GFF, GTF, or BED)
 *
 * @param id A name, presumably unique within a gene dataset, of a Gene
 * @param names Common names for the gene, possibly shared with other genes (for historical or
 *              ad hoc reasons)
 * @param strand The strand of the Gene (this is from data, not derived from the Transcripts' strand(s), and
 *               we leave open the possibility that a single Gene will have Transcripts in _both_ directions,
 *               e.g. anti-sense transcripts)
 * @param transcripts The Transcripts that are part of this gene model
 */
case class Gene(id: String, names: Seq[String], strand: Boolean, transcripts: Iterable[Transcript]) {

}

/**
 * A transcript model (here represented as a value of the Transcript class) is a simple,
 * hierarchical model containing a collection of exon models as well as an associated
 * gene identifier, transcript identifier, and a set of common names (synonyms).
 *
 * @param id the (unique) identifier of the Transcript
 * @param names Common names for the transcript
 * @param geneId The (unique) identifier of the gene to which the transcript belongs
 * @param exons The set of exons in the transcript model; each of these contain a
 *              reference region whose coordinates are in genomic space.
 * @param cds the set of CDS regions (the subset of the exons that are coding) for this
 *            transcript
 * @param utrs
 */
case class Transcript(id: String,
                      names: Seq[String],
                      geneId: String,
                      strand: Boolean,
                      exons: Iterable[Exon],
                      cds: Iterable[CDS],
                      utrs: Iterable[UTR]) {

}

/**
 * An exon model (here represented as a value of the Exon class) is a representation of a
 * single exon from a transcript in genomic coordinates.
 *
 * NOTE: we're not handling shared exons here
 *
 * @param transcriptId the (unique) identifier of the transcript to which the exon belongs
 * @param region The region (in genomic coordinates) to which the exon maps
 */
case class Exon(exonId: String, transcriptId: String, strand: Boolean, region: ReferenceRegion) {
}

/**
 * Coding Sequence annotations, should be a subset of an Exon for a particular Transcript
 * @param transcriptId
 * @param strand
 * @param region
 */
case class CDS(transcriptId: String, strand: Boolean, region: ReferenceRegion) {
}

/**
 * UnTranslated Regions
 *
 * @param transcriptId
 * @param strand
 * @param region
 */
case class UTR(transcriptId: String, strand: Boolean, region: ReferenceRegion) {
}

