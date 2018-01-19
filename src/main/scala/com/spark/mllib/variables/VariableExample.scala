package com.spark.mllib.variables

object VariableExample {
 
  def main(args: Array[String]) {

    import org.apache.spark.mllib.linalg.{ Vector, Vectors }
    import org.apache.spark.mllib.regression.LabeledPoint

    // create dense vector(1.0,0.0,3.0)
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)

    // Create a labeled point with a positive label and a dense feature vector.
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

    // Create a labeled point with a negative label and a sparse feature vector.
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))

  }
}