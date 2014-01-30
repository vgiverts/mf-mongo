package com.marsfog.mongo

import com.marsfog.data.MfDb
import com.mongodb.casbah.MongoDB
import com.mongodb.casbah.Imports._
import com.marsfog.reflect.Reflect
import java.lang.String

/**
 * Created by IntelliJ IDEA.
 * User: vgiverts
 * Date: 5/17/11
 * Time: 11:06 AM
 */

class MongoDbCollectionMapper(db: MongoDB, reflect: Reflect, objMgr: MongoObjectManager, ctxId: String) extends MfDb {

  def mapDelete(mapPk: Any, mapProperty: String, mapModel: Class[_], pk: Any) =
    getStorage(mapModel).mapDelete(mapPk, mapProperty, mapModel, pk)

  def mapPut(mapPk: Any, mapProperty: String, mapModel: Class[_], value: AnyRef, pk: Option[Any] = None) =
    getStorage(mapModel).mapPut(mapPk, mapProperty, mapModel, value, pk)

  def save(obj: AnyRef) = getStorage(obj.asInstanceOf[Object].getClass).save(obj)

  def save(modelName: String, obj: AnyRef) = getStorage(modelName).save(modelName, obj)

  def update(obj: AnyRef) = getStorage(obj.asInstanceOf[Object].getClass).update(obj)

  def setProperty(pk: Any, model: Class[_], propName: String, propValue: Any) =
    getStorage(model).setProperty(pk, model, propName, propValue)

  def setProperties(pk: Any, model: Class[_], fields: (String, Any)*) =
    getStorage(model).setProperties(pk, model, fields: _*)

  def append(pk: Any, model: Class[_], propName: String, propValues: Any*) =
    getStorage(model).append(pk, model, propName, propValues: _*)

  def pull(pk: Any, model: Class[_], propName: String, pullIdName: String, pullIdValue: Any) =
    getStorage(model).pull(pk, model, propName, pullIdName, pullIdValue)

  def pull(pk: Any, model: Class[_], propName: String, pullValue: Any) =
    getStorage(model).pull(pk, model, propName, pullValue)

  def deleteProperty(pk: Any, model: Class[_], propName: String) =
    getStorage(model).deleteProperty(pk, model, propName)

  def delete(pk: Any, model: Class[_]) = getStorage(model).delete(pk, model)

  def get[T <: AnyRef](property: (String, Any), model: Class[T]) = getStorage(model).get(property, model)

  def get[T <: AnyRef](pk: Any, model: Class[T]) = getStorage(model).get(pk, model)

  def get[T <: AnyRef](pk: Any, fields: Seq[(String, Any)], model: Class[T]) = getStorage(model).get(pk, fields, model)

  def get[T <: AnyRef](pks: Seq[Any], model: Class[T]) = getStorage(model).get(pks, model)

  def get[T <: AnyRef](pk: Any, modelName: String) = getStorage(modelName).get(pk, modelName)

  def increment(modelName: String, pk: Any, propName: String, delta: Long) =
    getStorage(modelName).increment(modelName, pk, propName, delta)

  def checkAndSetProperty(pk: Any, propName: String, model: Class[_], check: Any, set: Any) =
    getStorage(model).checkAndSetProperty(pk, propName, model, check, set)

  def getSorted[T <: AnyRef](modelName: String, propName: String, order: Int = 1, num: Option[Int] = None) =
    getStorage(modelName).getSorted(modelName, propName, order, num)

  def ensureIndex(modelName: String, propName: String, order: Int = 1) =
    getStorage(modelName).ensureIndex(modelName, propName, order)

  private def getStorage(model: Class[_]): MongoCollectionDb =
    getStorage(reflect.getClassInfo(model).shortClassName)

  private def getStorage(collection: String): MongoCollectionDb =
    new MongoCollectionDb(db.getCollection(collection + "_" + ctxId).asScala, objMgr)

}
