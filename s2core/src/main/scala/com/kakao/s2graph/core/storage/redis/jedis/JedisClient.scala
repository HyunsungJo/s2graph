package com.kakao.s2graph.core.storage.redis.jedis

import _root_.redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import com.kakao.s2graph.core.GraphUtil
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisException

import scala.collection.JavaConversions._
import scala.util.Try

/**
 * @author Junki Kim (wishoping@gmail.com) and Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Jan/07.
 */
class JedisClient(config: Config) {
  // TODO: refactor me
  lazy val instances: List[(String, Int)] = if (config.hasPath("redis.instances")) {
    (for {
      s <- config.getStringList("redis.storages")
    } yield {
        val sp = s.split(':')
        (sp(0), if (sp.length > 1) sp(1).toInt else 6379)
      }).toList
  } else List("localhost" -> 6379)


  private val log = LoggerFactory.getLogger(getClass)

  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(150)
  poolConfig.setMaxIdle(50)
  poolConfig.setMaxWaitMillis(200)

  val jedisPools = instances.map { case (host, port) =>
    new JedisPool(poolConfig, host, port)
  }

  def getBucketIdx(key: String): Int = {
    GraphUtil.murmur3(key) % jedisPools.size
  }

  def doBlockWithIndex[T](idx: Int)(f: Jedis => T): Try[T] = {
    Try {
      val pool = jedisPools(idx)

      var jedis: Jedis = null

      try {
        jedis = pool.getResource

        f(jedis)
      }
      catch {
        case e: JedisException =>
          pool.returnBrokenResource(jedis)

          jedis = null
          throw e
      }
      finally {
        if (jedis != null) {
          pool.returnResource(jedis)
        }
      }
    }
  }

  def doBlockWithKey[T](key: String)(f: Jedis => T): Try[T] = {
    doBlockWithIndex(getBucketIdx(key))(f)
  }

}

