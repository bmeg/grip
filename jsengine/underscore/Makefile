build : underscore.js
	go-bindata -nocompress -pkg underscore -o underscore.go underscore.js

underscore.js :
	curl http://underscorejs.org/underscore-min.js | gunzip > underscore.js
