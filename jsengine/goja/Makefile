
underscore.go : underscore.js
	go-bindata -nocompress -pkg goja -o underscore.go underscore.js

underscore.js :
	curl http://underscorejs.org/underscore-min.js | gunzip > underscore.js
	

tools:
	go get -u github.com/jteeuwen/go-bindata/...
