
Test Graph Database server based on Ophion protocol

To Install
----------
```
curl -O https://raw.githubusercontent.com/bmeg/arachne/master/contrib/Makefile
make download
```



To Run Larger 'Amazon Data Test'
--------------------------------

Turn on local arachne server


Download test data
```
curl -O http://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz
```

Load data

```
python src/github.com/bmeg/arachne/test/test_amazon_load.py amazon-meta.txt.gz  http://localhost:8000
```

Do queries

```
time python src/github.com/bmeg/arachne/test/test_amazon_queries.py http://localhost:8000
```