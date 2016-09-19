package com.sics.cgraph

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/** Enable Kryo serialization of classes here */
class CKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
	kryo.register(classOf[CGraph])
    kryo.register(classOf[GraphBuilder])
    kryo.register(classOf[Experiments])
  }
}