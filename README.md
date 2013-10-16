openidm-repo-mongodb
============

This is OpenIDM repository bundle to use MongoDB.

Build
------------
1. Check out OpenIDM source code (version 2.1.0-SNAPSHOT) from subversion.
2. Check out this repository under "openidm-project".
3. Add <module>openidm-repo-mongodb</module> in openidm-project/pom.xml

  ```xml
  <modules>
    <module>openidm-audit</module>
    <module>openidm-cluster</module>
    ......
    <module>openidm-repo-mongodb</module>
  </modules>
  ```
4. Maven install. (If you want to test, you must run mongodb in your local)

  ```shell
  mvn clean install -pl openidm-repo-mongodb -am
  ```


Getting start with OpenIDM
-----------
1. Copy openidm-repo-mongodb-2.1.0-SNAPSHOT.jar to *openidm-install-path*/bundle/
2. Copy MongoDB Java Driver, too.
3. Copy openidm-repo-mongodb/src/test/resources/repo.mongodb.json to openidm to *openidm-install-path*/conf/
4. Remove *openidm-install-path*/conf/repo.orientdb.json
5. Edit *openidm-install-path*/bin/laucher.json

  ```json
  {
    "bundle":{
      "containers":[
        {
          "location":"bundle",
          "includes":[
            "**/openidm-system-*.jar",
            "**/javax.transaction-*.jar"
          ],
          "start-level":1,
          "action":"install.start"
        },
        .....
        {
          "location":"bundle",
          "includes":[
            "**/openidm-repo-jdbc*.jar",
            "**/openidm-repo-orientdb*.jar",
            "**/openidm-repo-mongodb*.jar", << add this line
            "**/org.apache.felix.scr-*.jar"
          ],
          "start-level":4,
          "action":"install.start"
        },
        .....
        {
          "location":"bundle",
          "includes":["*.jar"],
          "excludes":[
            "**/openidm-security-*.jar",
            ...
            "**/openidm-repo-mongodb*.jar", << add this line
            ...
  ```
6. run MongoDB and OpenIDM.
