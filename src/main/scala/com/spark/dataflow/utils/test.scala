package com.spark.dataflow.utils

object test {
  // Main method
  def main(args: Array[String]) {

    // Using Some class
    val some: Option[String] = Some("15")

    // Using None class
    val none: Option[Int] = None

    // Applying getOrElse method
    val x = some.getOrElse(0)
    val y = none.getOrElse(17)

    // Displays the key in the
    // class Some
    println(x)

    // Displays default value
    println(y)
  }
}