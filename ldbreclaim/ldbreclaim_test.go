package main

import (
	"testing"
)

func TestVarious(t *testing.T) {
	f1 := Ldb { name: "f1",	size: 42, startKey: "a", endKey: "d", numKeys: 10, numDeletedKeys: 5}
	f2 := Ldb { name: "f2",	size: 42, startKey: "e", endKey: "h", numKeys: 10, numDeletedKeys: 5}
	f3 := Ldb { name: "f3",	size: 42, startKey: "i", endKey: "l", numKeys: 10, numDeletedKeys: 5}
	f4 := Ldb { name: "f4",	size: 42, startKey: "m", endKey: "q", numKeys: 10, numDeletedKeys: 5}
	f5 := Ldb { name: "f5",	size: 42, startKey: "c", endKey: "k", numKeys: 10, numDeletedKeys: 5}
	LdbAdd(f1); LdbAdd(f2); LdbAdd(f3); LdbAdd(f4); LdbAdd(f5)
	ComputeAverageKeySize()
	LdbSortByStartKey()
	i := LdbFindStartKey("b")
	if i != 1 {
                t.Errorf("Error LdbFindStartKey %v", i)
                return
        }
	FindRanges()
	LdbSortByNumKeys()
	DumpRanges()
}
