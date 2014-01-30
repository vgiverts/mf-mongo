package com.marsfog.data

import java.lang.String

/**
 * Created by IntelliJ IDEA.
 * User: vgiverts
 * Date: 3/28/11
 * Time: 9:39 PM
 *
 *
 */
// todo: refactor to use manifests!
trait MfDb {

  def get[T <: AnyRef](pk: Any, model: Class[T]): Option[T]

  def get[T <: AnyRef](pk: Any, fields: Seq[(String, Any)], model: Class[T]): Option[T]

  def get[T <: AnyRef](pk: Any, modelName: String): Option[T]

  def get[T <: AnyRef](pks: Seq[Any], model: Class[T]): Seq[Option[T]]

  def get[T <: AnyRef](property: (String, Any), model: Class[T]): Option[T]

  def delete(pk: Any, model: Class[_]): Boolean

  def save(obj: AnyRef): Boolean

  def save(modelName:String, obj: AnyRef): Boolean

  def update(obj: AnyRef): Boolean

  def setProperty(pk: Any, model: Class[_], propName: String, propValue: Any): Boolean

  def append(pk: Any, model: Class[_], propName: String, propValues: Any*): Boolean

  // todo: untested
  def pull(pk: Any, model: Class[_], propName: String, pullIdName:String, pullIdValue: Any): Boolean

  def pull(pk: Any, model: Class[_], propName: String, pullValue: Any): Boolean

  def setProperties(pk: Any, model: Class[_], fields: (String, Any)*): Boolean

  def deleteProperty(pk: Any, model: Class[_], propName: String): Boolean

  def mapPut(mapPk: Any, mapProperty: String, mapModel: Class[_], value: AnyRef, valuePk:Option[Any] = None): Boolean

  def mapDelete(mapPk: Any, mapProperty: String, mapModel: Class[_], pk: Any): Boolean

  def increment(modelName: String, pk: Any, propName:String, delta: Long): Boolean

  def checkAndSetProperty(pk: Any, propName: String, model: Class[_], check: Any, set: Any): Boolean

  def getSorted[T <: AnyRef](modelName: String, propName: String, order: Int = 1, num: Option[Int] = None): Seq[T]

  def ensureIndex(modelName: String, propName: String, order: Int = 1)

}
