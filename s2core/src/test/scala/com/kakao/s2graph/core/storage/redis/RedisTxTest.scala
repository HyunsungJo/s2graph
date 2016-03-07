package com.kakao.s2graph.core.storage.redis

import java.util.concurrent.TimeUnit

import com.kakao.s2graph.core.Integrate.IntegrateCommon
import com.kakao.s2graph.core.{Edge, Management, V3Test}
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Created by jojo on 3/7/16.
 */
class RedisTxTest extends IntegrateCommon with BeforeAndAfterEach {
  import TestUtil._

  case class PartialFailureException(edge: Edge, statusCode: Byte, failReason: String) extends Exception

  test("Redis tx", V3Test) {

    val expEdge = Management.toEdge(1L, "update", "e", "1", "1", testLabelNameV3, "{}")
    val expSnapshot = expEdge.toSnapshotEdge
    val expectedOpt = RedisSnapshotEdgeSerializable(expSnapshot).toKeyValues.headOption

    val reqEdge1 = Management.toEdge(2L, "update", "e", "2", "2", testLabelNameV3, "{")

    val reqEdge2 = Management.toEdge(3L, "update", "e", "3", "3", testLabelNameV3, "{")

    val writes = Set(reqEdge1, reqEdge2).map { rqe =>
      val reqSnapshot = rqe.toSnapshotEdge
      val rq = RedisSnapshotEdgeSerializable(reqSnapshot).toKeyValues.head

      val x = graph.storage.writeLock(rq, expectedOpt)
      Await.result(x, Duration(20, TimeUnit.SECONDS))
    }
    writes.map(println(_))
    writes.filter(x => x).size should be(1)

  }
}
