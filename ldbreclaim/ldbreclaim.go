// The ldbreclaim finds the most interesting ranges to compact
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/golang/leveldb/db"
	"github.com/golang/leveldb/table"
)

var (
	verifyChecksums = flag.Bool("c", false, "Verify checksums")
	verbose = flag.Bool("v", false, "Verbose")
	dumpStats = flag.Bool("s", false, "Dump stats and exits")
	numResults = flag.Int("n", 10, "Num results")
	tempSpaceMb = flag.Int64("t", 500, "Max overwrite desired in MB")
	minReclaimMb = flag.Int64("r", 300, "Minimum space to reclaim in MB")
)

func usage() {
	fmt.Printf("usage: ldbreclaim [options] folder (use --help for options)\n")
	os.Exit(1)
}

/*
func SetVerbose(value bool) {
	*verbose = value
}
*/

type Ldb struct {
	name string
	size int64
	startKey string
	endKey string
	numKeys int64 // setKeys + deletedKeys
	numDeletedKeys int64
	index int // for FindRange
}

// By is the type of a "less" function that defines the ordering of its Ldb arguments.
type By func(p1, p2 *Ldb) bool

// Sort is a method on the function type, By, that sorts the argument slice according to the function.
func (by By) Sort(s []Ldb) {
	ps := &ldbSorter{
		ldbs:    s,
		by:      by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ps)
}

// ldbSorter joins a By function and a slice of Ldbs to be sorted.
type ldbSorter struct {
	ldbs []Ldb
	by      func(p1, p2 *Ldb) bool // Closure used in the Less method.
}

// Len is part of sort.Interface.
func (s *ldbSorter) Len() int {
	return len(s.ldbs)
}

// Swap is part of sort.Interface.
func (s *ldbSorter) Swap(i, j int) {
	s.ldbs[i], s.ldbs[j] = s.ldbs[j], s.ldbs[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *ldbSorter) Less(i, j int) bool {
	return s.by(&s.ldbs[i], &s.ldbs[j])
}

var _ldbs []Ldb
var totalLdbs int64 = 0
var totalSize int64 = 0
var totalKeys int64 = 0
var totalDeletedKeys int64 = 0
var averageKeySize int64 = 0

func DumpStats() {
	fmt.Printf("totalLdbs=%v\n", totalLdbs)
	fmt.Printf("totalSize=%v bytes\n", totalSize)
	fmt.Printf("totalKeys=%v bytes\n", totalKeys)
	fmt.Printf("totalDeletedKeys=%v\n", totalDeletedKeys)
	fmt.Printf("averageKeySize=%v\n", averageKeySize)
}

func LdbAdd(ldb Ldb) {
	_ldbs = append(_ldbs, ldb)
	totalLdbs++
	totalSize += ldb.size
	totalKeys += ldb.numKeys
	totalDeletedKeys += ldb.numDeletedKeys
}

func ComputeAverageKeySize() {
	averageKeySize = totalSize / totalKeys
}

func LdbSortByStartKey() {
	// sort keys
	startKey := func(p1, p2 *Ldb) bool {
		return p1.startKey < p2.startKey
	}
	By(startKey).Sort(_ldbs)
}

func LdbSortByNumKeys() {
	// sort keys
	numKeys := func(p1, p2 *Ldb) bool {
		return p1.numKeys > p2.numKeys
	}
	By(numKeys).Sort(_ldbs)
}

// find the first ldb that starts after or at prefix
func LdbFindStartKey(prefix string) int {
	startKey := func(i int) bool {
		return _ldbs[i].startKey >= prefix
	}
	return sort.Search(len(_ldbs), startKey)
}

/*
func LdbDump(s []Ldb) {
	for i, ldb := range s {
		fmt.Printf("%v %v\n", i, ldb)
	}
}
*/

// remove ranges when endKey is < key
func RemoveRangesBefore(s []Ldb, key string) []Ldb {
	tmp := s[:0]
	for _, ldb := range s {
		if ldb.endKey >= key {
			tmp = append(tmp, ldb)
		}
	}
	return tmp
}

func AddToRanges(s []Ldb, numKeys int64, numDeletedKeys int64) {
	for _, ldb := range s {
		_ldbs[ldb.index].numKeys += numKeys
		_ldbs[ldb.index].numDeletedKeys += numDeletedKeys
	}
}

func FindRanges() {
	var currentRanges []Ldb

	for i, ldb := range _ldbs {
		currentRanges = RemoveRangesBefore(currentRanges, ldb.startKey)
		AddToRanges(currentRanges, ldb.numKeys, ldb.numDeletedKeys)
		newLdb := ldb
		newLdb.index = i
		currentRanges = append(currentRanges, newLdb)
	}
}

// call after being sorted by numKeys
func DumpRanges() {
	var resultsFound int = 0
	for _, ldb := range _ldbs {
		if *verbose {
			fmt.Printf("%v\n", ldb)
		}
		worstCaseSpaceNeededMb := (ldb.numKeys * averageKeySize) / (1024 * 1024)
		spaceReclaimedMb := (ldb.numDeletedKeys * averageKeySize) / (1024 * 1024)
		if spaceReclaimedMb > *minReclaimMb &&
			worstCaseSpaceNeededMb < *tempSpaceMb {
			fmt.Printf("%v\n", ldb)
			resultsFound++
		}
		if resultsFound >= *numResults {
			break
		}
	}
}

func Kind(k []byte) uint8 {
        return k[len(k)-8]
}

func analyzeLdb(folder string, filename string) error {
	path := folder + "/" + filename
	// Stat the path because it could be a symlink
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	// No need to "defer f.Close()", as closing r will close f.
	r := table.NewReader(f, &db.Options{
		VerifyChecksums: *verifyChecksums,
	})
	defer r.Close()

	ldb := Ldb {
		name: filename,
		size: fi.Size(),
		startKey: "",
		endKey: "",
		numKeys: 0,
		numDeletedKeys: 0,
	}

	var startKeyInitialized bool = false

	t := r.Find(nil, nil)
	for t.Next() {
		k := t.Key()
		kind := Kind(k)
		ldb.numKeys++
		var n int
		if kind == 0 {
			n = bytes.IndexByte(k, 0)
			ldb.numDeletedKeys++
		} else {
			n = bytes.IndexByte(k, 1)
		}
		if !startKeyInitialized {
			ldb.startKey = string(k[:n])
			startKeyInitialized = true
		}
		ldb.endKey = string(k[:n])
		// fmt.Printf("%q (%d): %q\n", k, len(k), kind)
	}
	if *verbose {
		fmt.Printf("%+v\n", ldb)
	}
	LdbAdd(ldb)
	return t.Close()
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		usage()
	}

	folder := args[0]
	files, err := ioutil.ReadDir(folder)
	if err != nil {
		fmt.Printf("error: %q\n", err)
		os.Exit(1)
	}

	for _, f := range files {
		matched, err := filepath.Match("*.ldb", f.Name())
		if err != nil {
			fmt.Printf("error: %q\n", err)
			os.Exit(1)
		}
		if matched {
			if err := analyzeLdb(folder, f.Name()); err != nil {
				fmt.Printf("analyzeLdb warning: %s %q\n", f.Name(), err)
			}
		}
	}

	if totalKeys == 0 {
		os.Exit(0)
	}
	ComputeAverageKeySize()
	
	if (*dumpStats) {
		DumpStats()
		os.Exit(0)
	}
	
	LdbSortByStartKey()
	FindRanges()
	LdbSortByNumKeys()
	DumpRanges()

	os.Exit(0)
}
