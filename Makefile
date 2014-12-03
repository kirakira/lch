.PHONY: all

all:
	mkdir -p bin/
	javac -d bin -cp bin src/*.java
