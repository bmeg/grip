

```
curl -O https://www.antlr.org/download/antlr-4.7.1-complete.jar
```

```
curl -O https://s3.amazonaws.com/artifacts.opencypher.org/M14/Cypher.g4
```


```
java -jar antlr-4.7.1-complete.jar -Dlanguage=Go -o parser Cypher.g4 
```
