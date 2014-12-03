.PHONY: all

all:
	mkdir -p bin/
	javac -d bin -cp bin -Xlint src/*.java
