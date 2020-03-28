
# pip install grpcio-tools

grpc:
	@cd ../multidriver && python -m grpc_tools.protoc -I ./ -I../../googleapis --python_out=../test-tools --grpc_python_out=../test-tools multidriver.proto
