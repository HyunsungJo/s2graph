package com.kakao.s2graph.core.Integrate

import java.util.concurrent.TimeUnit

import com.kakao.s2graph.core.{GraphUtil, V3Test}
import org.apache.hadoop.hbase.util.Bytes
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

class StrongLabelDeleteTest extends IntegrateCommon {

  import StrongDeleteUtil._
  import TestUtil._

  ignore("Strong consistency select", V3Test) {
    insertEdgesSync(bulkEdges(): _*)

    var result = getEdgesSync(query(0))
    (result \ "results").as[List[JsValue]].size should be(2)
    result = getEdgesSync(query(10))
    (result \ "results").as[List[JsValue]].size should be(2)
  }

  ignore("Strong consistency deleteAll", V3Test) {
    val deletedAt = 100
    var result = getEdgesSync(query(20, direction = "in", columnName = testColumnName))

//    println(result)
    (result \ "results").as[List[JsValue]].size should be(3)

    val deleteParam = Json.arr(
      Json.obj("label" -> testLabelNameV3,
        "direction" -> "in",
        "ids" -> Json.arr("20"),
        "timestamp" -> deletedAt))

    deleteAllSync(deleteParam)

    result = getEdgesSync(query(11, direction = "out"))
//    println(result)
    (result \ "results").as[List[JsValue]].size should be(0)

    result = getEdgesSync(query(12, direction = "out"))
//    println(result)
    (result \ "results").as[List[JsValue]].size should be(0)

    result = getEdgesSync(query(10, direction = "out"))
//    println(result)
    // 10 -> out -> 20 should not be in result.
    (result \ "results").as[List[JsValue]].size should be(1)
    (result \\ "to").size should be(1)
    (result \\ "to").head.as[Long] should be(21l)

    result = getEdgesSync(query(20, direction = "in", columnName = testColumnName))
//    println(result)
    (result \ "results").as[List[JsValue]].size should be(0)

    insertEdgesSync(bulkEdges(startTs = deletedAt + 1): _*)

    result = getEdgesSync(query(20, direction = "in", columnName = testColumnName))
//    println(result)

    (result \ "results").as[List[JsValue]].size should be(3)
  }


  test("update delete", V3Test) {
    val ret = for {
      i <- 0 until testNum
    } yield {
        val src = System.currentTimeMillis()

//        val (ret, last) = testInner(i, src)
        val (ret, last) = testInnerFail(i, src)
        ret should be(true)
        ret
      }

    ret.forall(identity)
  }

  ignore("update delete 2", V3Test) {
    val src = System.currentTimeMillis()
    var ts = 0L

    val ret = for {
      i <- 0 until testNum
    } yield {
        val (ret, lastTs) = testInner(ts, src)
        val deletedAt = lastTs + 1
        val deletedAt2 = lastTs + 2
        ts = deletedAt2 + 1 // nex start ts

        ret should be(true)

        val deleteAllRequest = Json.arr(Json.obj("label" -> labelName, "ids" -> Json.arr(src), "timestamp" -> deletedAt))
        val deleteAllRequest2 = Json.arr(Json.obj("label" -> labelName, "ids" -> Json.arr(src), "timestamp" -> deletedAt2))

        val deleteRet = deleteAllSync(deleteAllRequest)
        val deleteRet2 = deleteAllSync(deleteAllRequest2)

        val result = getEdgesSync(query(id = src))
//        println(result)

        val resultEdges = (result \ "results").as[Seq[JsValue]]
        resultEdges.isEmpty should be(true)

        val degreeAfterDeleteAll = getDegree(result)

        degreeAfterDeleteAll should be(0)
        degreeAfterDeleteAll === (0)
      }

    ret.forall(identity)
  }

  /** This test stress out test on degree
    * when contention is low but number of adjacent edges are large
    * Large set of contention test
    */
  ignore("large degrees", V3Test) {
    val labelName = testLabelNameV3
    val dir = "out"
    val maxSize = 100
    val deleteSize = 10
    val numOfConcurrentBatch = 100
    val src = System.currentTimeMillis()
    val tgts = (0 until maxSize).map { ith => src + ith }
    val deleteTgts = Random.shuffle(tgts).take(deleteSize)
    val insertRequests = tgts.map { tgt =>
      Seq(tgt, "insert", "e", src, tgt, labelName, "{}", dir).mkString("\t")
    }
    val deleteRequests = deleteTgts.take(deleteSize).map { tgt =>
      Seq(tgt + 1000, "delete", "e", src, tgt, labelName, "{}", dir).mkString("\t")
    }
    val allRequests = Random.shuffle(insertRequests ++ deleteRequests)
    //        val allRequests = insertRequests ++ deleteRequests
    val futures = allRequests.grouped(numOfConcurrentBatch).map { bulkRequests =>
      insertEdgesAsync(bulkRequests: _*)
    }

    Await.result(Future.sequence(futures), Duration(20, TimeUnit.MINUTES))

    val expectedDegree = insertRequests.size - deleteRequests.size
    val queryJson = query(id = src)
    val result = getEdgesSync(queryJson)
    val resultSize = (result \ "size").as[Long]
    val resultDegree = getDegree(result)

    //        println(result)

    val ret = resultSize == expectedDegree && resultDegree == resultSize
//    println(s"[MaxSize]: $maxSize")
//    println(s"[DeleteSize]: $deleteSize")
//    println(s"[ResultDegree]: $resultDegree")
//    println(s"[ExpectedDegree]: $expectedDegree")
//    println(s"[ResultSize]: $resultSize")
    ret should be(true)
  }

  ignore("deleteAll", V3Test) {
    val labelName = testLabelNameV3
    val dir = "out"
    val maxSize = 100
    val deleteSize = 10
    val numOfConcurrentBatch = 100
    val src = System.currentTimeMillis()
    val tgts = (0 until maxSize).map { ith => src + ith }
    val deleteTgts = Random.shuffle(tgts).take(deleteSize)
    val insertRequests = tgts.map { tgt =>
      Seq(tgt, "insert", "e", src, tgt, labelName, "{}", dir).mkString("\t")
    }
    val deleteRequests = deleteTgts.take(deleteSize).map { tgt =>
      Seq(tgt + 1000, "delete", "e", src, tgt, labelName, "{}", dir).mkString("\t")
    }
    val allRequests = Random.shuffle(insertRequests ++ deleteRequests)
    val futures = allRequests.grouped(numOfConcurrentBatch).map { bulkRequests =>
      insertEdgesAsync(bulkRequests: _*)
    }

    Await.result(Future.sequence(futures), Duration(20, TimeUnit.MINUTES))

    val deletedAt = System.currentTimeMillis()
    val deleteAllRequest = Json.arr(Json.obj("label" -> labelName, "ids" -> Json.arr(src), "timestamp" -> deletedAt))

    deleteAllSync(deleteAllRequest)

    val result = getEdgesSync(query(id = src))
//    println(result)
    val resultEdges = (result \ "results").as[Seq[JsValue]]
    resultEdges.isEmpty should be(true)

    val degreeAfterDeleteAll = getDegree(result)
    degreeAfterDeleteAll should be(0)
  }

  object StrongDeleteUtil {

    val labelName = testLabelNameV3
    val maxTgtId = 10
    val batchSize = 5
    val testNum = 2
    val numOfBatch = 5

    def testInner(startTs: Long, src: Long) = {
      val labelName = testLabelNameV3
      val lastOps = Array.fill(maxTgtId)("none")
      var currentTs = startTs
      println(s"currentTs ${src}: ${GraphUtil.bytesToHexString(Bytes.toBytes(src))}")

      val allRequests = for {
        ith <- 0 until numOfBatch
        jth <- 0 until batchSize
      } yield {
          currentTs += 1

          val tgt = Random.nextInt(maxTgtId)
          val op = if (Random.nextDouble() < 0.5) "delete" else "update"

          lastOps(tgt) = op
          println(s"${currentTs} ${src+tgt}: ${GraphUtil.bytesToHexString(Bytes.toBytes(src+tgt))}")
          Seq(currentTs, op, "e", src, src + tgt, labelName, "{}").mkString("\t")
        }

      println("In Order:")
      allRequests.foreach(println(_))
      val shuffled = Random.shuffle(allRequests)
      println("Shuffled:")
      shuffled.foreach(println _)

      val futures = shuffled.grouped(batchSize).map { bulkRequests =>
        insertEdgesAsync(bulkRequests: _*)
      }

      Await.result(Future.sequence(futures), Duration(20, TimeUnit.MINUTES))

      val expectedDegree = lastOps.count(op => op != "delete" && op != "none")
      val queryJson = query(id = src)
      val result = getEdgesSync(queryJson)
      val resultSize = (result \ "size").as[Long]
      val resultDegree = getDegree(result)

//      println(lastOps.toList)
//      println(result)
//      println(s">> ${Json.prettyPrint(result)}")
//      println(s">> resultDegree : $resultDegree, expectedDegree : $expectedDegree, resultSize : $resultSize")

      val ret = resultDegree == expectedDegree && resultSize == resultDegree
      if (!ret) System.err.println(s"[Contention Failed]: $resultDegree, $expectedDegree\n")
      else println(s"[Contention Success]: $resultDegree, $expectedDegree\n")

      (ret, currentTs)
    }


    def testInnerFail(startTs: Long, src: Long) = {
      val lastOps = Array.fill(maxTgtId)("none")
      val currentTs = startTs
      val allRequests = IndexedSeq(
        Seq(startTs, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+1, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+2, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+3, "delete", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+4, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+5, "delete", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+6, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+7, "delete", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+8, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+9, "delete", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+10, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+11, "delete", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+12, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+13, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+14, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t"),
        Seq(startTs+15, "update", "e", src, 1457324321121L, "s2graph_label_test_v3", "{}").mkString("\t")
      )
      allRequests.foreach(println _)
//      val futures = Random.shuffle(allRequests).grouped(batchSize).map { bulkRequests =>
//        insertEdgesAsync(bulkRequests: _*)
//      }
      val futures = allRequests.grouped(batchSize).map { bulkRequests =>
        insertEdgesAsync(bulkRequests: _*)
      }


      Await.result(Future.sequence(futures), Duration(20, TimeUnit.MINUTES))

      val expectedDegree = 1
      val queryJson = query(id = src)
      val result = getEdgesSync(queryJson)
      val resultSize = (result \ "size").as[Long]
      val resultDegree = getDegree(result)

      //      println(lastOps.toList)
      //      println(result)
      //      println(s">> ${Json.prettyPrint(result)}")
      //      println(s">> resultDegree : $resultDegree, expectedDegree : $expectedDegree, resultSize : $resultSize")

      val ret = resultDegree == expectedDegree && resultSize == resultDegree
      if (!ret) System.err.println(s"[Contention Failed]: $resultDegree, $expectedDegree")
      else println(s"[Contention Success]: $resultDegree, $expectedDegree")

      (ret, currentTs)

    }

    def bulkEdges(startTs: Int = 0) = Seq(
      toEdge(startTs + 1, "insert", "e", "0", "1", testLabelNameV3, s"""{"time": 10}"""),
      toEdge(startTs + 2, "insert", "e", "0", "1", testLabelNameV3, s"""{"time": 11}"""),
      toEdge(startTs + 3, "insert", "e", "0", "1", testLabelNameV3, s"""{"time": 12}"""),
      toEdge(startTs + 4, "insert", "e", "0", "2", testLabelNameV3, s"""{"time": 10}"""),
      toEdge(startTs + 5, "insert", "e", "10", "20", testLabelNameV3, s"""{"time": 10}"""),
      toEdge(startTs + 6, "insert", "e", "10", "21", testLabelNameV3, s"""{"time": 11}"""),
      toEdge(startTs + 7, "insert", "e", "11", "20", testLabelNameV3, s"""{"time": 12}"""),
      toEdge(startTs + 8, "insert", "e", "12", "20", testLabelNameV3, s"""{"time": 13}""")
    )

    def query(id: Long, serviceName: String = testServiceName, columnName: String = testColumnName,
              labelName: String = testLabelNameV3, direction: String = "out") = Json.parse(
      s"""
          { "srcVertices": [
            { "serviceName": "$serviceName",
              "columnName": "$columnName",
              "id": $id
             }],
            "steps": [
            [ {
                "label": "$labelName",
                "direction": "${direction}",
                "offset": 0,
                "limit": -1,
                "duplicate": "raw"
              }
            ]]
          }""")

    def getDegree(jsValue: JsValue): Long = {
      ((jsValue \ "degrees") \\ "_degree").headOption.map(_.as[Long]).getOrElse(0L)
    }
  }


}
