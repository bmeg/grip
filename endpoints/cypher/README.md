

# Prototype

This is a proof of concept and does not work.

## Notes for building code

```
curl -O https://www.antlr.org/download/antlr-4.10.1-complete.jar
```

```
curl -O https://s3.amazonaws.com/artifacts.opencypher.org/M18/Cypher.g4
```

```
java -jar antlr-4.10.1-complete.jar -Dlanguage=Go -o parser Cypher.g4 
```
