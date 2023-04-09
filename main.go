package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/dgraph-io/badger/v2"
)

func main() {
	args := os.Args[1:]
	fmt.Println(args)
	dbDir := args[0]

	opts := badger.DefaultOptions(dbDir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Set your prefix list here
	prefixes := args[1:] //[]string{"prefix1:", "prefix2:", "prefix3:"}

	topP, err := analyzeTopPrefixes(db, 30)
	for key, count := range topP {
		if count > 5 {
			fmt.Println(key, count)
		}
	}

	for _, prefix := range prefixes {
		prefixCount, prefixSize, err := analyzePrefix(db, prefix)
		if err != nil {
			log.Fatalf("Error analyzing prefix '%s': %v\n", prefix, err)
		}

		fmt.Printf("Prefix '%s': %d keys, %d bytes\n", prefix, prefixCount, prefixSize)
	}
}

func analyzeTopPrefixes(db *badger.DB, prefixLength int) (map[string]int, error) {
	prefixCount := make(map[string]int)

	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			//		prefix := string(key)[:min(len(key), prefixLength)]
			prefix := findPrefixUpToSlash(string(key), prefixLength)

			if _, exists := prefixCount[prefix]; exists {
				prefixCount[prefix]++
			} else {
				prefixCount[prefix] = 1
			}
		}
		return nil
	})

	return prefixCount, err
}

func findPrefixUpToSlash(key string, maxPrefixLength int) string {
	actualLength := min(len(key), maxPrefixLength)
	prefixEnd := strings.LastIndex(key[:actualLength], "/")
	if prefixEnd == -1 {
		return key[:actualLength]
	}
	return key[:prefixEnd+1]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func analyzePrefix(db *badger.DB, prefix string) (int, int, error) {
	var count int
	var size int

	err := db.View(func(txn *badger.Txn) error {
		prefixBytes := []byte(prefix)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			item := it.Item()
			key := item.Key()
			if strings.HasPrefix(string(key), prefix) {
				count++
				valueSize := item.ValueSize()
				size += len(key) + int(valueSize)
			}
		}

		return nil
	})

	return count, size, err
}
