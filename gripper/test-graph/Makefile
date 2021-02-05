
# pip install grpcio-tools

grpc:
	@cd ../ && python -m grpc_tools.protoc -I ./ -I../googleapis --python_out=test-graph --grpc_python_out=test-graph gripper.proto

graph:
	@./make-table-graph.py ../../conformance/graphs/swapi.vertices ../../conformance/graphs/swapi.edges swapi.edge-plan swapi
