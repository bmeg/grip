
## Notes for building code

```
curl -O https://www.antlr.org/download/antlr-4.13.2-complete.jar
```

```
curl -O https://s3.amazonaws.com/artifacts.opencypher.org/M23/Cypher.g4
```

```
java -jar antlr-4.13.2-complete.jar -Dlanguage=Go -o parser Cypher.g4 
```
