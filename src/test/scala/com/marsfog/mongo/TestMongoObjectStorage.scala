package com.marsfog.mongo

import com.mongodb.DBCollection
import com.mongodb.casbah.{MongoConnection, MongoDB}
import com.mongodb.casbah.Imports._
import org.specs._
import com.marsfog.reflect.ReflectionManager

/**
 * Created by IntelliJ IDEA.
 * User: vgiverts
 * Date: 5/13/11
 * Time: 9:38 AM
 */
// todo: add more test cases
object TestMongoObjectStorage extends Specification {

  val db: MongoDB = MongoConnection()("test")
  val coll: DBCollection = db.getCollection("test")
  val objMgr: MongoObjectManager = new MongoObjectManager(ReflectionManager, "com.marsfog.data.mongo.")
  val storage: MongoCollectionDb = new MongoCollectionDb(coll.asScala, objMgr)

  var idCtr = 0

  def newId() = {
    idCtr = idCtr + 1
    idCtr
  }

  class Foo {
    val _id = newId
    val bar = "asdf"

    override def toString = "{_id:" + _id + ",bar:\"" + bar + "\"}"

    override def equals(obj: Any) = obj.isInstanceOf[Foo] && obj.asInstanceOf[Foo]._id == _id && obj.asInstanceOf[Foo].bar == bar
  }

  "MongoCollectionDb" should {
    "save and retrieve simple objects" in {
      val foo: Foo = new Foo
      storage.save(foo) must be (true)
      storage.get(foo._id, classOf[Foo]) must beEqual (Some(foo))
      storage.delete(foo._id, classOf[Foo]) must beEqual (true)
      storage.get(foo._id, classOf[Foo]) must beEqual (None)
    }


  }

}