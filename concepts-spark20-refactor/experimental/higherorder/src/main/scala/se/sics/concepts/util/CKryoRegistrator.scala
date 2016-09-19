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

package se.sics.concepts.util

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import se.sics.concepts.higherorder.{Formula, HigherOrderConcept}

/** Kryo serialization registrator. */
class CKryoRegistrator extends KryoRegistrator {
  /** Enable Kryo serialization of classes here. */
  override def registerClasses(kryo: Kryo) {
    // Our classes
    kryo.register(classOf[Formula])
    kryo.register(classOf[HigherOrderConcept])
    // kryo.register(classOf[EdgeData])

    // Classes that occur in calculations but are not registered elsewhere
    kryo.register(classOf[scala.collection.mutable.PriorityQueue[Any]])
//    kryo.register(classOf[scala.collection.mutable.PriorityQueue$ResizableArrayAccess])
//    kryo.register(classOf[scala.collection.mutable.WrappedArray$ofRef])
    kryo.register(classOf[scala.collection.immutable.::[Any]])
//    kryo.register(classOf[scala.collection.immutable.Nil$])
//    kryo.register(classOf[scala.collection.immutable.Set$EmptySet$])
//    kryo.register(classOf[scala.collection.immutable.HashSet$HashSet1])
//    kryo.register(classOf[scala.collection.immutable.HashSet$EmptyHashSet$])
    kryo.register(classOf[scala.collection.immutable.Queue[Long]])
//    kryo.register(classOf[scala.None$])
    kryo.register(classOf[scala.math.BigDecimal])
//    kryo.register(classOf[scala.math.Ordering$Double$])
//    kryo.register(classOf[scala.math.Ordering$$anon$4])
//    kryo.register(classOf[scala.math.Ordering$$anon$9])
//    kryo.register(classOf[scala.math.Ordering$$anonfun$by$1])
    // kryo.register(classOf[com.sics.concepts.core$$anonfun$42$$anonfun$apply$2])
//    kryo.register(classOf[scala.reflect.ManifestFactory$$anon$10])
    kryo.register(classOf[Array[Object]])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Tuple2[Any, Any]]])
    kryo.register(classOf[Array[Tuple3[Any, Any, Any]]])
//    kryo.register(classOf[scala.reflect.ClassTag$$anon$1])
    kryo.register(classOf[java.lang.Class[Any]])
    kryo.register(classOf[java.math.BigDecimal])
    kryo.register(classOf[java.math.MathContext])
    kryo.register(classOf[java.math.RoundingMode])
    kryo.register(classOf[play.api.libs.json.JsObject])
    kryo.register(classOf[play.api.libs.json.JsString])
    kryo.register(classOf[play.api.libs.json.JsNumber])
    kryo.register(classOf[Array[play.api.libs.json.JsObject]])
  }
}
