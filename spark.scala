def getFileContentAsRDD(s: String) = {
  val html = scala.io.Source.fromURL(s"https://raw.githubusercontent.com/pakkio/tmp/master/$s").mkString
  val list = html.split("\n").filter(_ != "")
  sc.parallelize(list)
}
// Get json from my github pakkio/tmp project
def getGitJson(session: SparkSession, s: String) = {   
  session.read.json(getFileContentAsRDD(s)
}
                   
def getStringData(session: SparkSession, s: String) = {
   session.read.text(getFileContentAsRDD(s))
}

val spark = SparkSession
   .builder().getOrCreate()
val orgs = getGitJson(spark,"organizations.json")
val analyticsData = getStringData(spark,"analytics.json")
