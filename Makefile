@:
	go run main.go -old=./files/old.csv -new=./files/new.csv -key=reference

seeds:
	@ echo
	@ echo "Building seeds for benchmark..."
	@ echo
	@ go run ./seed/main.go

benchmark:
	@ echo
	@ echo "Starting running branchmarks..."
	@ echo
	@ go test ./diff -bench=.
