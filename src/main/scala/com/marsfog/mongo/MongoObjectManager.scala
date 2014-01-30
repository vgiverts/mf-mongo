package com.marsfog.mongo

import com.mongodb.DBObject
import java.lang.String

import scala.collection.JavaConversions._
import com.mongodb.casbah.Imports._
import collection.immutable.Map
import com.marsfog.reflect.{PrimitiveValue, ReflectionManager, Reflect}

/**
 * Created by IntelliJ IDEA.
 * User: vgiverts
 * Date: 5/10/11
 * Time: 1:07 PM
 */

class MongoObjectManager(reflect: Reflect, packagePrefix: String) {

  def toJava(mongoVal: Any): AnyRef = toJava(mongoVal, null)

  def toJava(mongoVal: Any, exptectedType: Class[_]): AnyRef = {
    mongoVal match {
      case x: BasicDBList => x.toList.map(v => toJava(v))
      case x: DBObject => toJava(x, exptectedType)
      case x: ObjectId => x
      case ClassValue(clazz) => clazz
      case x: AnyRef => x
      case null if exptectedType == null => null
      case null => reflect.getClassInfo(exptectedType).getDefaultValue.asInstanceOf[AnyRef]
      case _ => throw new IllegalArgumentException("Unexpected type returned from MongoDB.")
    }
  }

  private def toJava(mongoObj: DBObject): AnyRef = toJava(mongoObj, null)

  private def toJava(mongoObj: DBObject, exptectedType: Class[_]): AnyRef = {
    if (mongoObj == null) {
      null
    } else {
      val simpleClassName: AnyRef = mongoObj.get(ReflectionManager.TYPE_FIELD)
      if ((exptectedType == null || exptectedType == classOf[Object] || classOf[scala.collection.Map[_,_]].isAssignableFrom(exptectedType)) && simpleClassName == null) {
        mongoObj.toMap.map(e => (toJava(e._1) -> toJava(e._2))).toMap
      } else {
        val javaObj: AnyRef =
          (if (exptectedType != null) reflect.newInstance(exptectedType)
          else reflect.newInstance(packagePrefix + simpleClassName)).asInstanceOf[AnyRef]
        reflect.getFields(javaObj).foreach(f => {
          val a: AnyRef = mongoObj.get(f.getName)
          val b: Class[_] = f.getType.clazz
          f.convertAndSet(javaObj, toJava(a, b))
        })
        javaObj
      }
    }
  }

  def toJavaMap(mongoObj: DBObject): Map[String, AnyRef] =
    Map(mongoObj.keySet.filter(_ != "_id").map(key => key -> toJava(mongoObj.get(key).asInstanceOf[DBObject])).toSeq: _*)

  def wrapPk(pk: Any) = MongoDBObject("_id" -> pk)

  def wrapProp(prop: Tuple2[String, Any]) = MongoDBObject(prop)

  def wrapProps(props: Seq[(String, Any)]) = MongoDBObject(props.toList)

  def toMongo(value: Any, requireId: Boolean = false): Any = {
    value match {
      case x: String => x
      case x: Map[_, _] => new BasicDBObject(x.map(e => (toMongo(e._1), toMongo(e._2))))
      case list: List[_] =>
        val mongoList = new BasicDBList()
        list.foldLeft(0)((idx, v) => {
          mongoList.put(idx, toMongo(v))
          idx + 1
        })
        mongoList
      case x: Class[_] => "_c" + x.getName
      case PrimitiveValue(x) => value
      case x: ObjectId => value
      case x: Tuple2[_,_] => new BasicDBObject(Map(toMongo(x._1) -> toMongo(x._2)))
      case x: AnyRef => new MongoWrapperObject(x, reflect, this, requireId)
      case _ => value
    }
  }

  def getIdString(value: AnyRef): String = reflect.getField(value, "_id").map(_.get(value)).map(toIdString)
      .getOrElse(throw new IllegalArgumentException("Value must have a '_id' field."))

  def toIdString(pk: Any): String = pk match {
    case x: ObjectId => "ObjectId(" + x.toString + ")"
    case x => x.toString
  }
}

object ClassValue {
  def unapply(str: String) = if (str.startsWith("_c")) Some(ReflectionManager.getClassInfo(str.substring(2)).clazz) else None
}

