grpc:
	@echo Generating files:
	python -m grpc_tools.protoc -I eventproc/protobuff --python_out=eventproc/protobuff --grpc_python_out=eventproc/protobuff eventproc/protobuff/api.proto
	@echo
	@echo Fixing import statements:
	sed -i'.bak' 's/btrdb_pb2 as btrdb__pb2/btrdb.grpcinterface.btrdb_pb2 as btrdb__pb2/' btrdb/grpcinterface/btrdb_pb2_grpc.py

