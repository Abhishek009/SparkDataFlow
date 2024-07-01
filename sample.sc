import scala.io
import upickle.default._

val input  = io.Source.
  fromFile("D:\\Google_Drive_Rahul\\GitHub\\SparkDataFlow\\src\\main\\resources\\meta.conf").mkString

/*val keyValuePairs = input
  .split("\n")
  .map(_.split("="))
  .map { case Array(k, v) => (k.stripPrefix("\"").stripSuffix("\""), v.stripPrefix("\"").stripSuffix("\"")) }
  .toMap

println(keyValuePairs)*/
//val jsonStr = """{"ods":"202406191","schema":"SOME_SCHEMA_NAME"}"""
println(s"""{${input.replace("\n",",")}}""")
val input1=s"""{${input.replace("\n",",")}}"""
// Deserialize the content into a Map[String, String]
val map: Map[String, String] = read[Map[String, String]](input1)