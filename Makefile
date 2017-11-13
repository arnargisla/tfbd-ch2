all: build run

run: Main.class
	java -classpath ".:sqlite-jdbc-3.20.0.jar" Main

run-parallel: MainParallel.class
	java -classpath ".:sqlite-jdbc-3.20.0.jar" MainParallel

build MainParallel.class Main.class:
	javac *.java

