SRC = $(wildcard *.puml)
all: $(SRC:.puml=.png)
%.png: %.puml; plantuml $<
