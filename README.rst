studyTeam -- proxy access to e-compliance DB SQL Server DB via HTTP

by Dan Connolly, KUMC Medical Informatics
Copyright (c) 2016 University of Kansas Medical Center


Build-time Configuration
------------------------

See `ecomplaince.properties.example` for a template for database
access details. The `http.port` should also be set here.

Then `sbt package` should work as usual.

*TODO: Add a build file to set project name.*


Usage
-----

To start the service:

  java -jar studyTeam.jar --serve

To look up members of a study by ID:

  curl http://127.0.0.1:8080?id=12337


Design, Documentation, and Testing
----------------------------------

See the main class, `kumc_bmi.studyTeam.StudyTeam`, for details,
including integration testing.

Unit tests are not provided.

Use `sbt doc` to build API documentation.
