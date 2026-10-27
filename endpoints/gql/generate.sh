#!/bin/sh

git clone https://github.com/opengql/grammar.git

cd grammar && java -jar ../antlr-4.13.2-complete.jar -Dlanguage=Go -o ../parser GQL.g4 
