lazy val root = (project in file("."))
  .settings(
    name := "studyTeam",
    scalaVersion := "2.12.7",

    // https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
    libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.0.jre8"
  )
