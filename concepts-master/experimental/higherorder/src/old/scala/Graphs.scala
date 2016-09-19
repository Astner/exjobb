/*
 Copyright (C) 2015 Daniel Gillblad, Olof Görnerup , Theodoros Vasiloudis (dgi@sics.se,
 olofg@sics.se, tvas@sics.se).

 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package se.sics.concepts.remove

import org.apache.spark.rdd.RDD

/** Graph related functionality for the concepts library. */
object Graphs {
  /** Join vertex and edge values.
  *
  * Join values vi and vj of vertices i and j with values vij of edges (i,j).
  * Return (i,j,vi,vj,vij) tuples.
  */
  def joinVertexAndEdgeValues(vertices : RDD[(Long, Double)], edges : RDD[(Long, Long, Double)]) :
    RDD[(Long, Long, Double, Double, Double)] = {
      edges.map{case (i,j,vij)=>(i,(j,vij))}.join(vertices).
            map{case (i,((j,vij),vi))=>(j,(i,vij,vi))}.join(vertices).
            map{case (j,((i,vij,vi),vj))=>(i,j,vi,vj,vij)}
    }

  /** Join vertex and edge values.
  *
  * Join values vi and vj of vertices i and j with values vij of edges (i,j).
  * Return (i,j,vi,vj,vij) tuples.
  */
  def joinVertexAndEdgePairsValues(vertices : RDD[(Long, Double)], edges : RDD[((Long, Long), Double)]) :
    RDD[(Long, Long, Double, Double, Double)] = {
      edges.map{case ((i,j),vij)=>(i,(j,vij))}.join(vertices).
            map{case (i,((j,vij),vi))=>(j,(i,vij,vi))}.join(vertices).
            map{case (j,((i,vij,vi),vj))=>(i,j,vi,vj,vij)}
    }

}
