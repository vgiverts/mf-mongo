package com.marsfog.mongo

import com.marsfog.data.MfDb
import com.mongodb.casbah.Imports._
import com.mongodb.DBCollection
import com.mongodb.casbah.MongoDB
import com.marsfog.reflect.ReflectionManager
import java.lang.String
import scala.collection.JavaConversions._

/**
 * Created by IntelliJ IDEA.
 * User: vgiverts
 * Date: 5/10/11
 * Time: 11:11 AM
 */

class MongoCollectionDb(coll: MongoCollection, objMgr: MongoObjectManager) extends MfDb {

  def mapDelete(mapPk: Any, mapProperty: String, mapModel: Class[_], pk: Any) =
    deleteProperty(mapPk, mapModel, mapProperty + "." + objMgr.toIdString(pk))

  def mapPut(mapPk: Any, mapProperty: String, mapModel: Class[_], value: AnyRef, pk: Option[Any] = None) =
    setProperty(mapPk, mapModel, mapProperty + "." + pk.map(objMgr.toIdString(_)).getOrElse(objMgr.getIdString(value)), value)

  def delete(pk: Any, model: Class[_]) = wrapError(coll.remove(objMgr.wrapPk(pk)))

  def save(obj: AnyRef): Boolean = wrapError(coll.save(objMgr.toMongo(obj, true).asInstanceOf[DBObject]))

  def save(modelName: String, obj: AnyRef) = save(obj)

  def update(obj: AnyRef) = wrapError(coll.update(
    objMgr.wrapPk(objMgr.getIdString(obj)),
    objMgr.toMongo(obj, true).asInstanceOf[DBObject]),
    false,
    false
  )

  def setProperty(pk: Any, model: Class[_], propName: String, propValue: Any) =
    wrapError(coll.update(objMgr.wrapPk(pk), $set(propName -> objMgr.toMongo(propValue))))

  def append(pk: Any, model: Class[_], propName: String, propValues: Any*) =
    wrapError(coll.update(
      objMgr.wrapPk(pk),
      if (propValues.length == 1) $push(propName -> objMgr.toMongo(propValues.head))
      else $pushAll(propName -> propValues.map(objMgr.toMongo(_))),
      true,
      false
    ))

  def pull(pk: Any, model: Class[_], propName: String, pullIdName: String, pullIdValue: Any) =
    wrapError(coll.update(objMgr.wrapPk(pk), $pull(propName -> (pullIdName -> pullIdValue))))

  def pull(pk: Any, model: Class[_], propName: String, pullValue: Any) =
    wrapError(coll.update(objMgr.wrapPk(pk), $pull(propName -> pullValue)))

  def setProperties(pk: Any, model: Class[_], fields: (String, Any)*) =
    wrapError(coll.update(objMgr.wrapPk(pk), $set(fields.map(f => (f._1, objMgr.toMongo(f._2))): _*)))

  def deleteProperty(pk: Any, model: Class[_], propName: String) =
    wrapError(coll.update(objMgr.wrapPk(pk), $unset(propName)))

  def get[T <: AnyRef](pk: Any, model: Class[T]): Option[T] = coll.findOne(objMgr.wrapPk(pk)).map(obj => objMgr.toJava(obj, model).asInstanceOf[T])

  def get[T <: AnyRef](pk: Any, fields: Seq[(String, Any)], model: Class[T]): Option[T] =
  {
    val wrappedFields: MongoDBObject = objMgr.wrapProps(fields)
    coll.findOne(objMgr.wrapPk(pk), wrappedFields).map(obj => objMgr.toJava(obj, model).asInstanceOf[T])
  }

  def get[T <: AnyRef](pk: Any, modelName: String) = coll.findOne(objMgr.wrapPk(pk)).map(obj => objMgr.toJava(obj).asInstanceOf[T])

  // todo: parallelize this
  def get[T <: AnyRef](pks: Seq[Any], model: Class[T]) = pks.map(pk => get(pk, model))

  def get[T <: AnyRef](property: (String, Any), model: Class[T]) =
    coll.findOne(objMgr.wrapProp(property)).map(obj => objMgr.toJava(obj).asInstanceOf[T])

  def increment(modelName: String, pk: Any, propName: String, delta: Long) =
    wrapError(coll.update(objMgr.wrapPk(pk), $inc(propName -> delta)))

  def checkAndSetProperty(pk: Any, propName: String, model: Class[_], check: Any, set: Any) =
    coll.update(MongoDBObject("_id" -> pk, propName -> objMgr.toMongo(check)), $set(propName -> objMgr.toMongo(set)))
      .getLastError(WriteConcern.Safe).get("updatedExisting").asInstanceOf[Boolean]


  def getSorted[T <: AnyRef](modelName: String, propName: String, order: Int = 1, num: Option[Int]):Seq[T] =
    coll.find().sort(MongoDBObject(propName -> order)).limit(num.getOrElse(0)).map(objMgr.toJava(_).asInstanceOf[T]).toSeq


  def ensureIndex(modelName: String, propName: String, order: Int = 1) =
    coll.ensureIndex(MongoDBObject(propName -> order))

  private def wrapError(op: => Any) = {
    // don't ignore exceptions!
    //    try {
    val ret = op;
    true;
    //    }
    //    //todo: log this
    //    catch {
    //      case e: Throwable => e.printStackTrace(); false
    //    }
  }


}

object Foo {
  var idCtr = 2

  def newId() = {
    idCtr = idCtr + 1
    idCtr
  }
}

class Foo {
  val _id = Foo.newId()
  val bar = "asdf"

  override def toString = "{_id:" + _id + ",bar:" + bar + "}"

  override def equals(obj: Any) = obj match {
    case x: Foo => x._id == _id && x.bar == bar
    case _ => false
  }
}

class Bar {
  var foo: Foo = null
  var map: Map[_, _] = null
  val _id = Foo.newId()

  override def toString = "{_id:" + _id + ",foo:" + foo + ",map:" + map + "}"

  override def equals(obj: Any) = obj match {
    case x: Bar => x.foo == foo && x.map == map && x._id == _id
    case _ => false
  }
}

object Main {
  def main(args: Array[String]) {
    val db: MongoDB = MongoConnection()("test")
    val coll: DBCollection = db.getCollection("test")
    val objMgr: MongoObjectManager = new MongoObjectManager(ReflectionManager, "com.marsfog.data.mongo.")
    val storage: MongoCollectionDb = new MongoCollectionDb(coll.asScala, objMgr)

    // Test save
    val foo1: Foo = new Foo
    val saved: Foo = foo1
    storage.save(saved)
    println("saved: " + saved)
    val savedId: Int = saved._id
    println("loaded: " + storage.get(savedId, classOf[Foo]))

    //    // Test map
    //    val foo2: Foo = new Foo
    //    storage.mapPut("map1", foo2)
    //    println("loaded map: " + storage.getMap("map1"))
    //    val foo3: Foo = new Foo
    //    storage.mapPut("map1", foo3)
    //    println("loaded map: " + storage.getMap("map1"))
    //    storage.mapDelete("map1", foo2._id)
    //    println("loaded map: " + storage.getMap("map1"))
    //    storage.deleteMap("map1")
    //    println("loaded map: " + storage.getMap("map1"))
    //
    //    // Test graph
    //    val bar = new Bar
    //    bar.foo = foo1
    //    bar.map = Map("y" -> "z", "mapFoo" -> foo1)
    //    storage.save(bar)
    //    println("saved bar: " + bar)
    //    println("loaded bar: " + storage.get(bar._id, classOf[Bar]).get)
    //    assert(bar == storage.get(bar._id, classOf[Bar]).get)

  }
}

