package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.Integrate.IntegrateCommon
import com.kakao.s2graph.core.rest.RequestParser
import com.kakao.s2graph.core.utils.logger
import com.kakao.s2graph.core.{Graph, Management, V3Test}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Created by jojo on 3/7/16.
 */
class RedisTxTest extends IntegrateCommon with BeforeAndAfterEach {

  import TestUtil._

  override def beforeAll = {
    config = ConfigFactory.load()
      .withValue("storage.engine", ConfigValueFactory.fromAnyRef("redis")) // for redis test
      .withValue("storage.redis.instances", ConfigValueFactory.fromIterable(List[String]("localhost"))) // for redis test
    graph = new Graph(config)(ExecutionContext.Implicits.global)
    parser = new RequestParser(graph.config)
    management = new Management(graph)
  }

  test("Redis tx", V3Test) {

    graph.storage.simpleWrite("key", "original")

    val results = for {
      i <- 0 until 20
    } yield {
        graph.storage.writeWithTx("key", i.toString, "original")
      }
    val rslt = Await.result(Future.sequence(results), Duration("10 seconds"))
    val size = rslt.filter(b => b).size
    logger.error(s"size: $size")
    size should be(1)
  }

  //  ignore("Redis tx", V3Test) {
  //
  //    val expEdge = Management.toEdge(1L, "update", "1", "1", testLabelNameV3, "out", "{}")
  //    val expSnapshot = expEdge.toSnapshotEdge
  //    val expectedOpt = RedisSnapshotEdgeSerializable(expSnapshot).toKeyValues.headOption
  //
  //    val reqEdge1 = Management.toEdge(2L, "update", "2", "2", testLabelNameV3, "out", "{}")
  //
  //    val reqEdge2 = Management.toEdge(3L, "update", "3", "3", testLabelNameV3, "out", "{}")
  //
  //    val writes = Set(reqEdge1, reqEdge2).map { rqe =>
  //      val reqSnapshot = rqe.toSnapshotEdge
  //      val rq = RedisSnapshotEdgeSerializable(reqSnapshot).toKeyValues.head
  //
  //      val x = graph.storage.writeLock(rq, expectedOpt)
  //      Await.result(x, Duration(20, TimeUnit.SECONDS))
  //    }
  //    writes.map(println(_))
  //    writes.filter(x => x).size should be(1)
  //
  //  }
}
