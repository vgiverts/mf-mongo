package com.marsfog.mongo

import java.lang.String
import org.bson.BSONObject
import scala.collection.JavaConversions._
import collection.immutable.HashMap
import com.mongodb.{BasicDBList, BasicDBObject, DBObject}
import com.marsfog.reflect.{FieldInfo, Reflect}

/**
 * Created by IntelliJ IDEA.
 * User: vgiverts
 * Date: 5/10/11
 * Time: 11:38 AM
 *
 * Used for saving only.
 */

class MongoWrapperObject(obj: Object, reflect: Reflect, mgr: MongoObjectManager, requireId:Boolean) extends DBObject {

  if (requireId && reflect.getField(obj, "_id").isEmpty)
    throw new IllegalArgumentException("All mongo objects must have an '_id' field.")

  def isPartialObject = false

  def markAsPartialObject() = throw new UnsupportedOperationException

  def keySet() = Set(reflect.getFields(obj).map(_.getName): _*)

  def containsField(fieldName: String) = reflect.getField(obj, fieldName).isDefined

  def containsKey(fieldName: String) = containsField(fieldName)

  def removeField(fieldName: String) = throw new UnsupportedOperationException

  def toMap = HashMap(reflect.getFields(obj).map(f => (f.getName, String.valueOf(getFieldVal(f)))): _*)

  def get(key: String) = reflect.getField(obj, key).map(f => getFieldVal(f)).getOrElse(null).asInstanceOf[AnyRef]

  def putAll(m: java.util.Map[_, _]) = throw new UnsupportedOperationException

  def putAll(o: BSONObject) = throw new UnsupportedOperationException

  def put(key: String, v: AnyRef) = throw new UnsupportedOperationException

  def getFieldVal(field: FieldInfo[_]): Any = mgr.toMongo(field.get(obj))

}

