package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.GraphUtil
import com.kakao.s2graph.core.storage.{SKeyValue, StorageDeserializable}
import com.kakao.s2graph.core.types.{LabelWithDirection, SourceVertexId, VertexId}
import com.kakao.s2graph.core.utils.logger
import org.apache.hadoop.hbase.util.Bytes


trait GDeserializable[E] extends StorageDeserializable[E] {
  import StorageDeserializable._

  type RowKeyRaw = (VertexId, LabelWithDirection, Byte, Boolean, Int)

  /** version 1 and version 2 share same code for parsing row key part */
  def parseRow(kv: SKeyValue, version: String): RowKeyRaw = {
    logger.info(s">> parse row ${GraphUtil.bytesToHexString(kv.row)}")
    var pos = 0
    val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(kv.row, pos, kv.row.length, version)
    pos += srcIdLen
    val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
    pos += 4
    val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsSnapshot(kv.row, pos)

    val rowLen = srcIdLen + 4 + 1
    (srcVertexId, labelWithDir, labelIdxSeq, isInverted, rowLen)
  }
}