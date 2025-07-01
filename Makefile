.PHONY: protoc

protoc:
	cd ./api && \
	protoc --go_out=../raft/grpc --go_opt=paths=source_relative \
		--go-grpc_out=../raft/grpc --go-grpc_opt=paths=source_relative \
		raft.proto
