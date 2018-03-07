package eventhub

// ptrBool takes a boolean and returns a pointer to that bool. For use in literal pointers, ptrBool(true) -> *bool
func ptrBool(toPtr bool) *bool {
	return &toPtr
}

// ptrString takes a string and returns a pointer to that string. For use in literal pointers,
// ptrString(fmt.Sprintf("..", foo)) -> *string
func ptrString(toPtr string) *string {
	return &toPtr
}

// ptrInt32 takes a int32 and returns a pointer to that int32. For use in literal pointers, ptrInt32(1) -> *int32
func ptrInt32(number int32) *int32 {
	return &number
}

// ptrInt64 takes a int64 and returns a pointer to that int64. For use in literal pointers, ptrInt64(1) -> *int64
func ptrInt64(number int64) *int64 {
	return &number
}
