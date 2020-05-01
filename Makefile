@:
	go run main.go -old=./files/old.csv -new=./files/new.csv -key=reference

benchmark:
	@ echo
	@ echo "Starting running branchmarks..."
	@ echo
	@ go test ./diff -bench=. -cpuprofile profile_cpu.out
