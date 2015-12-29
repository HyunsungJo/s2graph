package controllers

import play.api.mvc.{Action, Controller}
import redis.RedisClient

object RedisController extends Controller {
  implicit val akkaSystem = akka.actor.ActorSystem()
  import scala.concurrent.ExecutionContext.Implicits.global
  val redis = RedisClient()

  def ping = Action.async { request =>
    val res = redis.ping()
    res.map { r =>
      Ok(r)
    }
  }
}

