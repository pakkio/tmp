// create organizations from a url
val html = scala.io.Source.fromURL("https://raw.githubusercontent.com/pakkio/tmp/master/organizations.json").mkString
val list = html.split("\n").filter(_ != "")
val rddOrgJsonString = sc.parallelize(list)
val spark = SparkSession
   .builder().getOrCreate()
val rddOrganizations = spark.read.json(rddOrgJsonString)
rddOrganizations.show
