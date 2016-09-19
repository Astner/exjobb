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

package se.sics.concepts.io

object ProductIO {
  def toSimpleString(prod: Product, separator : Char = ' ') : String =
    prod.toString.filterNot(c=>c == '(' || c == ')').replace(',', separator)

  def toCSV(prod: Product) : String =
    prod.toString.filterNot(c=>c == '(' || c == ')')

  trait RE[T] { def read(str: String): T }

  object RE {
    implicit object ReadInt     extends RE[Int]     { def read(str: String) = str.toInt }
    implicit object ReadDouble  extends RE[Double]  { def read(str: String) = str.toDouble }
    implicit object ReadFloat   extends RE[Float]   { def read(str: String) = str.toFloat }
    implicit object ReadLong    extends RE[Long]    { def read(str: String) = str.toLong }
    implicit object ReadShort   extends RE[Short]   { def read(str: String) = str.toShort }
    implicit object ReadByte    extends RE[Byte]    { def read(str: String) = str.toByte }
    implicit object ReadBoolean extends RE[Boolean] { def read(str: String) = str.toBoolean }
    implicit object ReadChar    extends RE[Char]    { def read(str: String) = str(0) }
    implicit object ReadString  extends RE[String]  { def read(str: String) = str }
  }

  def fromString[T: RE](str: String) : T = implicitly[RE[T]].read(str)

  def fromStrings[T:RE,U:RE](strs : Seq[String]) : (T,U) =
    (implicitly[RE[T]].read(strs.head), implicitly[RE[U]].read(strs(1)))
  def fromStrings[T:RE,U:RE,V:RE](strs : Seq[String]) : (T,U,V) =
    (implicitly[RE[T]].read(strs.head), implicitly[RE[U]].read(strs(1)), implicitly[RE[V]].read(strs(2)))
  def fromStrings[T:RE,U:RE,V:RE,W:RE](strs : Seq[String]) : (T,U,V,W) =
    (implicitly[RE[T]].read(strs.head), implicitly[RE[U]].read(strs(1)),
     implicitly[RE[V]].read(strs(2)), implicitly[RE[W]].read(strs(3)))
  def fromStrings[T:RE,U:RE,V:RE,W:RE,X:RE](strs : Seq[String]) : (T,U,V,W,X) =
    (implicitly[RE[T]].read(strs.head), implicitly[RE[U]].read(strs(1)), implicitly[RE[V]].read(strs(2)),
     implicitly[RE[W]].read(strs(3)), implicitly[RE[X]].read(strs(4)))

  def fromCSV[T:RE,U:RE](str: String) : (T,U) = fromStrings[T,U](str.split(','))
  def fromCSV[T:RE,U:RE,V:RE](str: String) : (T,U,V) = fromStrings[T,U,V](str.split(','))
  def fromCSV[T:RE,U:RE,V:RE,W:RE](str: String) : (T,U,V,W) = fromStrings[T,U,V,W](str.split(','))
  def fromCSV[T:RE,U:RE,V:RE,W:RE,X:RE](str: String) : (T,U,V,W,X) = fromStrings[T,U,V,W,X](str.split(','))

  def fromSimpleString[T:RE,U:RE](str: String) : (T,U) = fromStrings[T,U](str.split(' '))
  def fromSimpleString[T:RE,U:RE,V:RE](str: String) : (T,U,V) = fromStrings[T,U,V](str.split(' '))
  def fromSimpleString[T:RE,U:RE,V:RE,W:RE](str: String) : (T,U,V,W) = fromStrings[T,U,V,W](str.split(' '))
  def fromSimpleString[T:RE,U:RE,V:RE,W:RE,X:RE] (str: String) : (T,U,V,W,X) =
    fromStrings[T,U,V,W,X](str.split(' '))

}
