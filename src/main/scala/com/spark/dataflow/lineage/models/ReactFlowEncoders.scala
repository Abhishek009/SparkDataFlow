package com.spark.dataflow.lineage.models

import com.spark.dataflow.lineage.models.NodeDetails
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

object ReactFlowEncoders {
  implicit val reactFlowStyleEncoder: Encoder[ReactFlowStyle] = deriveEncoder[ReactFlowStyle]
  implicit val nodeDetailsEncoder: Encoder[NodeDetails] = deriveEncoder[NodeDetails]
  implicit val reactFlowDataEncoder: Encoder[ReactFlowData] = deriveEncoder[ReactFlowData]
  implicit val reactFlowPositionEncoder: Encoder[ReactFlowPosition] = deriveEncoder[ReactFlowPosition]
  implicit val reactFlowNodeEncoder: Encoder[ReactFlowNode] = deriveEncoder[ReactFlowNode]
  implicit val reactFlowEdgeEncoder: Encoder[ReactFlowEdge] = deriveEncoder[ReactFlowEdge]
  implicit val pipelineMetadataEncoder: Encoder[PipelineMetadata] = deriveEncoder[PipelineMetadata]
  implicit val reactFlowConfigEncoder: Encoder[ReactFlowConfig] = deriveEncoder[ReactFlowConfig]
}
