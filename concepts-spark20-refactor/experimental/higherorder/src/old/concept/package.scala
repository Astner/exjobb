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

package se.sics

// TODO: Needs overhaul after refactoring is done

/** ==<i>Concepts - Higher-order concept discovery for Spark/Scala</i>==
  *
  * ==Background==
  *
  * Concepts is a library for object correlation, similarity, and higher-order
  * concept discovery in very large data sets.
  * It aims at being a straightforward, easy-to-use and complete solution for
  * <ul>
  * <li>Calculation of pairwise concept correlations using a number of correlation measures
  * <li>Perform efficient transformation from a correlation- to a similarity graph
  * <li>Perform different types of graph-clustering for higher order concept discovery
  * <li>Efficiently perform hierarchical discovery of both recurrent, AND and OR concepts
  * </ul>
  * <p>
  * <ul>
  * The implementation aims to be
  * <li>Fully scalable both in terms of input data, concepts, and discovered higher order concepts
  * <li>Fully modular in terms of correlation measures, similarity measures, clustering algorithms etc.
  * <li>Lightweight, based on a functional codebase where core functions have no side effects
  * <li>Potentially portable to other compute engines such as Flink
  * </ul>
  *
  * ==Basic concepts==
  *
  * The library relies on a few basic assumptions:
  * <ul>
  * <li>Representations (data sets, clusters, higher-order concepts etc.) are
  *     stored and exchanged as Spark RDDs to allow for scaling.
  * <li>All concepts, both basic (e.g. words, songs, codons, events) and higher-order,
  * are identified by a unique <i>concept ID</i> represented by a `Long` value.
  * <li>In all core library functions, data is represented by <i>(index, conceptID)</i> pairs
  *     indicating that <i>conceptID</i> occurred at example <i>index</i> in data.
  *     Just like the <i>conceptID</i>, the <i>index</i> is represented by a `Long`.
  *     Typical functionality for converting to- and from this data format can be found in
  *     [[se.sics.concepts.util]] and [[se.sics.concepts.main]].
  * <li>Clusters and higher-order concepts are represented as collections of the constituent
  *     concept IDs.
  * </ul>
  *
  * ==Library structure==
  *
  * The library is organised around a number of packages that can be included as necessary.
  *
  * <b>Graphs</b><p>
  * The [[se.sics.concepts.graphs]] package contains core graph related functionality for the library.
  *
  * <b>Higherorder</b><p>
  * The [[se.sics.concepts.higherorder]] package contains functionality for deriving and
  * utilizing higher order concepts.
  *
  * <b>Utilities</b><p>
  * The [[se.sics.concepts.util]] package contains commonly used utility functions for transforming data,
  * graphs etc.
  *
  * <b>IO</b><p>
  * The [[se.sics.concepts.io]] package contains commonly used io functions for reading and writing data,
  * graphs etc. on different formats.
  *
  * <b>Main</b><p>
  * The [[se.sics.concepts.main]] package contains test, evaluation, and related utility functions for the
  * library.
  *
  * <b>Measures</b><p>
  * The [[se.sics.concepts.measures]] package contains measures on standard format for
  * correlation graph derivation and package recurrence significance.
  *
  * <b>Clustering</b><p>
  * The [[se.sics.concepts.clustering]] package contains graph clustering functions on standard format for higher-order
  * concept discovery.
  *
  * <b>Classification</b><p>
  * The [[se.sics.concepts.classification]] package contains classification functions and classes for concept contexts.
  *
  * @author Daniel Gillblad
  * @author Olof Gornerup
  * @author Theodoros Vasiloudis
*/
package object concepts {}
