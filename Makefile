tiny:
	go run main.go -old=./files/tiny_old.csv -new=./files/tiny_new.csv -key=reference

large:
	go run main.go -old=./files/large_old.csv -new=./files/large_new.csv -key=reference

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
