studyTeam -- proxy access to e-compliance DB SQL Server DB via HTTP

by Dan Connolly, KUMC Medical Informatics
Copyright (c) 2016 University of Kansas Medical Center


Dependencies: JDBC driver
-------------------------

Note the `@Grab` annotation atop `StudyTeam.groovy`.


Configuration
-------------

See `ecomplaince.properties.example` for a template for database
access details. The `http.port` should also be set here.


Usage
-----

To start the service:

  groovy StudyTeam.groovy --config ecompliance.properties --serve

To look up members of a study by ID:

  curl http://127.0.0.1:8080?id=12337


Design, Documentation, and Testing
----------------------------------

See the main class, `kumc_bmi.studyTeam.StudyTeam`, for details,
including integration testing.
