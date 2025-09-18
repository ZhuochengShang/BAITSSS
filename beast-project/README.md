# Beast Project

To test your classes, you can simply run your main class from the IDE.

If you want to run an existing Beast function, you can create a run configuration that runs the main class
`edu.ucr.cs.bdlab.beast.operations.Main`. This is equivalent to the `beast` command and takes the same arguments.

To run your program from command line, use the packages `bin/beast` command.
```shell
mvn package
bin/beast target/beast-project-1.0-SNAPSHOT.jar
```

If you want to run existing Beast commands and include your project, using the beast command as follows.
```shell
mvn package
bin/beast --jars target/beast-project-1.0-SNAPSHOT.jar <additional Beast arguments>
```