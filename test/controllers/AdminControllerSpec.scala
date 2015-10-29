package controllers

import com.kakao.s2graph.core.mysqls.Label
import play.api.http.HeaderNames
import play.api.test.{FakeApplication, FakeRequest}
import test.controllers.SpecCommon
import play.api.test.Helpers._

import scala.concurrent.Await


/**
 * Created by mojo22jojo(hyunsung.jo@gmail.com) on 15. 10. 13..
 */
class AdminControllerSpec extends SpecCommon {
  init()
  "EdgeControllerSpec" should {
    "update htable" in {
      running(FakeApplication()) {
        val insertUrl = s"/graphs/updateHTable/$testLabelName/$newHTableName"

        val req = FakeRequest("POST", insertUrl).withBody("").withHeaders(HeaderNames.CONTENT_TYPE -> "text/plain")

        Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
        Label.findByName(testLabelName, useCache = false).get.hTableName mustEqual newHTableName
      }
    }

    "rename label" in {
      running(FakeApplication()) {
        var insertUrl = s"/graphs/renameLabel/$testLabelName/s2graph_label_test_temp"
        var req = FakeRequest("POST", insertUrl).withBody("").withHeaders(HeaderNames.CONTENT_TYPE -> "text/plain")

        Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
        Label.findByName(testLabelName, useCache = false) mustEqual None

        insertUrl = s"/graphs/renameLabel/$testLabelName/s2graph_label_test_temp"
        req = FakeRequest("POST", insertUrl).withBody("").withHeaders(HeaderNames.CONTENT_TYPE -> "text/plain")
        Await.result(route(req).get, HTTP_REQ_WAITING_TIME).toString() must contain("404")

        insertUrl = s"/graphs/renameLabel/s2graph_label_test_temp/$testLabelName"
        req = FakeRequest("POST", insertUrl).withBody("").withHeaders(HeaderNames.CONTENT_TYPE -> "text/plain")

        Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
        Label.findByName(testLabelName, useCache = false).get.label mustEqual testLabelName

      }
    }

    "swap labels" in {
      running(FakeApplication()) {
        var insertUrl = s"/graphs/swapLabels/$testLabelName/$testLabelName2"
        var req = FakeRequest("POST", insertUrl).withBody("").withHeaders(HeaderNames.CONTENT_TYPE -> "text/plain")

        Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
        Label.findByName(testLabelName2, useCache = false).get.hTableName mustEqual newHTableName

        Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
        Label.findByName(testLabelName, useCache = false).get.hTableName mustEqual newHTableName

        insertUrl = s"/graphs/swapLabels/badLabelName/$testLabelName2"
        req = FakeRequest("POST", insertUrl).withBody("").withHeaders(HeaderNames.CONTENT_TYPE -> "text/plain")
        Await.result(route(req).get, HTTP_REQ_WAITING_TIME).toString() must contain("404")

        insertUrl = s"/graphs/swapLabels/$testLabelName2/badLabelName"
        req = FakeRequest("POST", insertUrl).withBody("").withHeaders(HeaderNames.CONTENT_TYPE -> "text/plain")
        Await.result(route(req).get, HTTP_REQ_WAITING_TIME).toString() must contain("404")
      }
    }
  }
}
