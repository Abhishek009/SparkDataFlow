package com.spark.dataflow.lineage.models

case class ReactFlowConfig(
                            nodes: List[ReactFlowNode],
                            edges: List[ReactFlowEdge],
                            pipelineMetadata: PipelineMetadata
                          )
