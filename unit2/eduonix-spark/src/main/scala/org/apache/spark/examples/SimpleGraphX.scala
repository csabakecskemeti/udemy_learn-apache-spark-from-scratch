package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
;

/**
 * Simple GraphX application for Eduonix
 */
object SimpleGraphX {

    case  class ExposureVector(name: String, exposure: Int, inDeg: Int, outDeg: Int)

    def main(args: Array[String]) {

        val vertexArray = Array( (1L, ("Patient 1", 28)), (2L, ("Patient 2", 27)), (3L, ("Patient 3", 15)),
            (4L, ("Patient 4", 22)), (5L, ("Patient 5", 16)), (6L, ("Patient 6", 10))
        )
        val edgeArray = Array( Edge(2L, 1L, 7), Edge(2L, 4L, 2), Edge(3L, 2L, 4), Edge(3L, 6L, 3), Edge(4L, 1L, 1),
            Edge(5L, 2L, 2), Edge(5L, 3L, 8), Edge(5L, 6L, 3))

        val conf = new SparkConf().set("spark.locality.wait", "100000")
        conf.setMaster("local[*]")
        val sc = new SparkContext(conf.setAppName("SimpleGraphX)"))

        val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
        val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

        val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)



        for ((id,(name, exposure)) <- graph.vertices.filter { case (id,(name,exposure)) => exposure > 16 }.collect) {
            println(s"$name was  exposed $exposure days ago")
        }


        for (triplet <- graph.triplets.collect) {
            println(s"${triplet.dstAttr._1} exposed ${triplet.srcAttr._1}")
        }


        val inDegrees: VertexRDD[Int] = graph.inDegrees

        val initialExposurEGraph: Graph[ExposureVector, Int] = graph.mapVertices{ case (id, (name, exposure)) => ExposureVector(name, exposure, 0, 0) }

        val exposureGraph = initialExposurEGraph.outerJoinVertices(initialExposurEGraph.inDegrees) {
            case (id, u, inDegOpt) => ExposureVector(u.name, u.exposure, inDegOpt.getOrElse(0), u.outDeg)
        }.outerJoinVertices(initialExposurEGraph.outDegrees) {
            case (id, u, outDegOpt) => ExposureVector(u.name, u.exposure, u.inDeg, outDegOpt.getOrElse(0))
        }


        for ((id, property) <- exposureGraph.vertices.collect) {
            println(s"Entity $id is called ${property.name} and has been exposed by ${property.inDeg} people.")
        }

    }

}
