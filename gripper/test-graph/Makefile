
# pip install grpcio-tools

grpc:
	@cd ../ && python -m grpc_tools.protoc -I ./ -I../googleapis --python_out=test-graph --grpc_python_out=test-graph gripper.proto
