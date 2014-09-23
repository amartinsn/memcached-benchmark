name := "new-api"

version := "1.0"

scalaVersion := "2.10.3"

resolvers ++= Seq(
  "twttr" at "http://maven.twttr.com/",
  "Spy" at "http://files.couchbase.com/maven2/"
)

libraryDependencies ++= Seq(
  "com.bionicspirit" %% "shade" % "1.6.0",
  "com.twitter" % "finagle-core_2.10" % "6.20.0",
  "com.twitter" % "finagle-memcached_2.10" % "6.20.0",
  "com.twitter" % "finagle-http_2.10" % "6.20.0",
  "com.twitter" % "finagle-redis_2.10" % "6.20.0",
  "com.twitter" % "bijection-util_2.10" % "0.7.0"
)