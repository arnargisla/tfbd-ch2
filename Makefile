all: build run

run: Main.class
	java -classpath ".:sqlite-jdbc-3.20.0.jar" Main 10

build:
	javac *.java

