package dataframe

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"github.com/aggnr/bluejay/db" // Import the db package
)

const (
	treePercentage = 0.01 // 1% of total rows
	chunkSize      = 1000 // Number of records per chunk
	chunkDir       = "data" // Directory to store chunks
	maxCacheSize   = 1 << 30 // 1GB
)

type DataFrame struct {
	Name       string
	StructType reflect.Type
	Indexes    []*db.BPlusTree // Use multiple BPlusTrees
	mutex      sync.RWMutex
	numTrees   int
	chunkDir   string
	chunkCount int
	cache      map[int]map[int]interface{}
	cacheSize  int
}

func init() {
	gob.Register(&db.BPlusTree{})
	gob.Register(&db.BPlusTreeNode{})
	gob.Register(map[string]interface{}{})
}

func NewDataFrame(data interface{}) (*DataFrame, error) {
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Len() == 0 {
		return nil, fmt.Errorf("data slice is empty")
	}

	numTrees := int(float64(v.Len()) * treePercentage)
	if numTrees == 0 {
		numTrees = 1 // Ensure at least one tree
	}

	df := &DataFrame{
		Indexes:  make([]*db.BPlusTree, numTrees), // Initialize multiple BPlusTrees
		numTrees: numTrees,
		chunkDir: chunkDir,
		cache:    make(map[int]map[int]interface{}),
	}

	if err := df.createChunkDir(); err != nil {
		return nil, err
	}

	for i := 0; i < numTrees; i++ {
		df.Indexes[i] = db.NewBPlusTree(v.Len())
	}

	if err := df.FromStructs(data); err != nil {
		return nil, err
	}

	// Set a finalizer to ensure Close is called when df goes out of scope
	runtime.SetFinalizer(df, func(df *DataFrame) {
		df.Close()
	})

	return df, nil
}

// createChunkDir ensures the chunk directory exists.
func (df *DataFrame) createChunkDir() error {
	if _, err := os.Stat(df.chunkDir); os.IsNotExist(err) {
		if err := os.MkdirAll(df.chunkDir, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create chunk directory: %v", err)
		}
	}
	return nil
}

// FromStructs creates a DataFrame from a slice of structs.
func (df *DataFrame) FromStructs(data interface{}) error {
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Len() == 0 {
		return fmt.Errorf("data slice is empty")
	}

	elemType := v.Type().Elem()

	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("data must be a slice of structs")
	}

	df.StructType = elemType
	df.Name = elemType.Name()

	rows := make(map[int]interface{})
	for i := 0; i < v.Len(); i++ {
		structVal := v.Index(i)
		values := make(map[string]interface{})
		for j := 0; j < structVal.NumField(); j++ {
			values[structVal.Type().Field(j).Name] = structVal.Field(j).Interface()
		}
		rows[i] = values
	}

	df.InsertRows(rows)
	return nil
}

// InsertRows inserts multiple rows into the DataFrame concurrently.
func (df *DataFrame) InsertRows(rows map[int]interface{}) {
	var wg sync.WaitGroup

	for id, row := range rows {
		wg.Add(1)
		go func(id int, row interface{}) {
			defer wg.Done()
			df.InsertRow(id, row)
		}(id, row)
	}
	wg.Wait()
}

func (df *DataFrame) InsertRow(id int, row interface{}) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	chunkID := id / chunkSize
	if df.cache[chunkID] == nil {
		df.cache[chunkID] = make(map[int]interface{})
	}

	df.cache[chunkID][id] = row
	df.cacheSize += int(reflect.TypeOf(row).Size())

	if df.cacheSize >= maxCacheSize {
		df.flushCache()
	}

	treeIndex := id % df.numTrees
	df.Indexes[treeIndex].Insert(id) // Insert the key into the appropriate BPlusTree
}

func (df *DataFrame) ReadRow(id int) (interface{}, error) {
	df.mutex.RLock()
	defer df.mutex.RUnlock()

	treeIndex := id % df.numTrees
	if !df.Indexes[treeIndex].Search(id) {
		return nil, fmt.Errorf("row with id %d not found", id)
	}

	chunkID := id / chunkSize
	if chunk, exists := df.cache[chunkID]; exists {
		if row, exists := chunk[id]; exists {
			return row, nil
		}
	}

	chunkFile := filepath.Join(df.chunkDir, fmt.Sprintf("chunk_%d.gob", chunkID))
	chunk, err := df.readChunk(chunkFile)
	if err != nil {
		return nil, fmt.Errorf("error reading chunk file %s: %v", chunkFile, err)
	}

	row, exists := chunk[id]
	if !exists {
		return nil, fmt.Errorf("row with id %d not found", id)
	}
	return row, nil
}

func (df *DataFrame) flushCache() {
	for chunkID, chunk := range df.cache {
		chunkFile := filepath.Join(df.chunkDir, fmt.Sprintf("chunk_%d.gob", chunkID))
		existingChunk, err := df.readChunk(chunkFile)
		if err == nil {
			for id, row := range chunk {
				existingChunk[id] = row
			}
		} else {
			existingChunk = chunk
		}
		df.writeChunk(chunkFile, existingChunk)
	}
	df.cache = make(map[int]map[int]interface{})
	df.cacheSize = 0
}

func (df *DataFrame) writeChunk(filename string, chunk map[int]interface{}) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(chunk); err != nil {
		return err
	}
	return nil
}

func (df *DataFrame) readChunk(filename string) (map[int]interface{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var chunk map[int]interface{}
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&chunk); err != nil {
		return nil, err
	}
	return chunk, nil
}

// Close deletes all the disk caches and writes B+Trees to the data directory.
func (df *DataFrame) Close() {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	// Remove all chunk files
	err := os.RemoveAll(df.chunkDir)
	if err != nil {
		fmt.Printf("Error deleting chunk directory %s: %v\n", df.chunkDir, err)
	}

	// Recreate the chunk directory
	if err := os.MkdirAll(df.chunkDir, os.ModePerm); err != nil {
		fmt.Printf("Failed to recreate chunk directory: %v\n", err)
		return
	}

	//// Write each B+Tree to a file in the data directory
	//for i, tree := range df.Indexes {
	//	treeFile := filepath.Join(df.chunkDir, fmt.Sprintf("bplustree_%d.gob", i))
	//	file, err := os.Create(treeFile)
	//	fmt.Println("Writing B+Tree to file:", treeFile)
	//	if err != nil {
	//		fmt.Printf("Error creating B+Tree file %s: %v\n", treeFile, err)
	//		continue
	//	}
	//	defer file.Close()
	//
	//	encoder := gob.NewEncoder(file)
	//	if err := encoder.Encode(tree); err != nil {
	//		fmt.Printf("Error encoding B+Tree to file %s: %v\n", treeFile, err)
	//	}
	//}
}