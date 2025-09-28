package com.spark.dataflow.lineage.models

import com.spark.dataflow.lineage.models.NodeDetails

case class ReactFlowData(
                          label: String,
                          details: NodeDetails,
                          tInputs: Option[List[String]] = None // Added for transform nodes to define multiple input handles
                        )
