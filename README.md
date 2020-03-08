# Storm
This project requires a working storm cluster setup with Nimbus and Zookeeper.
Once that is setup you must take the project directory, and prepare it to be run.

Run
mvn clean
mvn install
mvn package
inside the top of the project directory.

Then you can create either a non-parallel or parallel topology.
Both with will give the top 100 hashtags over time every 10 seconds.
These will be saved to log files in a directory you specify as a parameter. 
You can also specify the eta and s values as parameters to the java run.
The commands look like the below.

Non-parallel:

\<path to storm\>/storm jar \<target jar with dependencies\> TwitTopology \<topology name\> \<path to output file\> \<eta\> \<s\> remote 

ex:

/s/chopin/l/grad/acf003/cs535/storm/apache-storm-2.1.0/bin/storm jar target/Storm-1.0-jar-with-dependencies.jar TwitTopology twit /s/chopin/l/grad/acf003/cs535/storm/Output/twitter.log 0.002 0.0021  remote



Parallel:

\<path to storm\>/storm jar \<target jar with dependencies\> ParallelTwitTopology \<topology name\> \<path to output file\> \<eta\> \<s\> remote 

ex:

/s/chopin/l/grad/acf003/cs535/storm/apache-storm-2.1.0/bin/storm jar target/Storm-1.0-jar-with-dependencies.jar ParallelTwitTopology ptwit /s/chopin/l/grad/acf003/cs535/storm/Output/parallel_twitter.log 0.002 0.0021 remote

You can kill the running topology with the below command:
\<path to storm\>/storm kill \<Topology Name\>

ex:

/s/chopin/l/grad/acf003/cs535/storm/apache-storm-2.1.0/bin/storm kill ptwit