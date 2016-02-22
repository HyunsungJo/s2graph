package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.types.{InnerVal, InnerValLike, VertexId}
import com.kakao.s2graph.core.{GraphUtil, QueryParam, Vertex}
import com.kakao.s2graph.core.storage.{CanSKeyValue, StorageDeserializable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * @author Junki Kim (wishoping@gmail.com), Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Feb/19.
 */

class RedisVertexDeserializable extends StorageDeserializable[Vertex] {

  override def fromKeyValues[T: CanSKeyValue](queryParam: QueryParam,
                                              _kvs: Seq[T],
                                              version: String,
                                              cacheElementOpt: Option[Vertex]): Vertex = {

    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }

    val kv = kvs.head
    val (vertexId, _) = VertexId.fromBytes(kv.row, -GraphUtil.bytesForMurMurHash, kv.row.length, version) // no hash bytes => offset: -2

    var maxTs = Long.MinValue
    val propsMap = new collection.mutable.HashMap[Int, InnerValLike]
    val belongLabelIds = new ListBuffer[Int]

    for {
      kv <- kvs
    } {
      var offset = 0
      val propKey = Bytes.toInt(kv.value)
      offset += 4
      val ts = Bytes.toLong(kv.value, offset, 8)
      offset += 8
      if (ts > maxTs) maxTs = ts

      if (Vertex.isLabelId(propKey)) {
        belongLabelIds += Vertex.toLabelId(propKey)
      } else {
        val (value, _) = InnerVal.fromBytes(kv.value, offset, kv.value.length, version)
        propsMap += (propKey -> value)
      }
    }
    assert(maxTs != Long.MinValue)
    Vertex(vertexId, maxTs, propsMap.toMap, belongLabelIds = belongLabelIds)
  }
}

