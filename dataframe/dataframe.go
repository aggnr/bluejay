package dataframe

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"github.com/aggnr/bluejay/db" // Import the db package
)

const (
	defaultRowChunkSize    = 10000 // Default size of each row chunk
	defaultColumnChunkSize = 10    // Default size of each column chunk
)

type DataFrame struct {
	mutex          sync.RWMutex
	Columns        map[string][]interface{}
	Index          *db.BPlusTree // Use BPlusTree from db package
	rowChunks      map[int]string
	columnChunks   map[int]string
	filePath       string
	storageOnDisk  bool // New flag to determine storage type
	StructType     reflect.Type // Add StructType field
	rowChunkSize   int
	columnChunkSize int
	inMemoryChunks map[int]map[string][]interface{} // Add in-memory chunks map
}

func NewDataFrame(data interface{}, filePath string, storageOnDisk bool) (*DataFrame, error) {
	rowChunkSize, columnChunkSize := calculateChunkSizes(data)

	df := &DataFrame{
		Columns:        make(map[string][]interface{}),
		Index:          db.NewBPlusTree(), // Initialize BPlusTree
		rowChunks:      make(map[int]string),
		columnChunks:   make(map[int]string),
		filePath:       filePath,
		storageOnDisk:  storageOnDisk,
		StructType:     reflect.TypeOf(data).Elem(), // Initialize StructType
		rowChunkSize:   rowChunkSize,
		columnChunkSize: columnChunkSize,
	}

	if err := df.FromStructs(data); err != nil {
		return nil, err
	}

	if storageOnDisk {
		if err := df.SaveChunksToDisk(); err != nil {
			return nil, err
		}
	}

	return df, nil
}

func calculateChunkSizes(data interface{}) (int, int) {
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Len() == 0 {
		return defaultRowChunkSize, defaultColumnChunkSize
	}

	// Example logic to calculate chunk sizes based on data size
	rowChunkSize := defaultRowChunkSize
	columnChunkSize := defaultColumnChunkSize

	if v.Len() > 100000 {
		rowChunkSize = 20000
		columnChunkSize = 20
	} else if v.Len() > 50000 {
		rowChunkSize = 15000
		columnChunkSize = 15
	}

	return rowChunkSize, columnChunkSize
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

	var wg sync.WaitGroup
	numWorkers := 100 // Number of concurrent workers
	chunkSize := (v.Len() + numWorkers - 1) / numWorkers

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > v.Len() {
			end = v.Len()
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				structVal := v.Index(i)
				for j := 0; j < structVal.NumField(); j++ {
					fieldName := structVal.Type().Field(j).Name
					df.mutex.Lock()
					df.Columns[fieldName] = append(df.Columns[fieldName], structVal.Field(j).Interface())
					df.mutex.Unlock()
				}
				rowChunkIndex := i / df.rowChunkSize
				columnChunkIndex := structVal.NumField() / df.columnChunkSize
				df.Index.Insert(i, rowChunkIndex, columnChunkIndex)
			}
		}(start, end)
	}

	wg.Wait()

	if df.storageOnDisk {
		return df.SaveChunksToDisk()
	}

	return nil
}


func (df *DataFrame) InsertRow(id int, row interface{}) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	structVal := reflect.ValueOf(row)
	for j := 0; j < structVal.NumField(); j++ {
		fieldName := structVal.Type().Field(j).Name
		df.Columns[fieldName] = append(df.Columns[fieldName], structVal.Field(j).Interface())
	}

	rowChunkIndex := id / df.rowChunkSize
	df.Index.Insert(id, rowChunkIndex, 0) // Insert the key into the BPlusTree

	if df.storageOnDisk {
		df.SaveRowChunkToDisk(rowChunkIndex)
	} else {
		df.InsertRowChunk(rowChunkIndex) // Use in-memory storage
	}
}

func (df *DataFrame) InsertRowChunk(chunkIndex int) {
	if df.inMemoryChunks == nil {
		df.inMemoryChunks = make(map[int]map[string][]interface{})
	}
	chunk := make(map[string][]interface{})
	for colName, colData := range df.Columns {
		start := chunkIndex * df.rowChunkSize
		end := start + df.rowChunkSize
		if end > len(colData) {
			end = len(colData)
		}
		chunk[colName] = colData[start:end]
	}
	df.inMemoryChunks[chunkIndex] = chunk // Store the chunk in-memory
}

func (df *DataFrame) ReadRow(id int) (map[string]interface{}, error) {
	df.mutex.RLock()
	defer df.mutex.RUnlock()
	chunkIndices, found := df.Index.Search(id) // Search for the key in the BPlusTree
	if !found {
		return nil, fmt.Errorf("row with ID %d not found", id)
	}

	rowChunkIndex := chunkIndices[0]

	if df.storageOnDisk {
		if err := df.LoadRowChunkFromDisk(rowChunkIndex); err != nil {
			return nil, err
		}
	} else {
		df.LoadRowChunk(rowChunkIndex) // Use in-memory loading
	}

	row := make(map[string]interface{})
	for colName, colData := range df.Columns {
		row[colName] = colData[id]
	}
	return row, nil
}

func (df *DataFrame) LoadRowChunk(chunkIndex int) error {
	chunk, exists := df.inMemoryChunks[chunkIndex]
	if !exists {
		return fmt.Errorf("chunk %d not found", chunkIndex)
	}
	for colName, colData := range chunk {
		df.Columns[colName] = append(df.Columns[colName][:chunkIndex*df.rowChunkSize], colData...)
	}
	return nil
}

func (df *DataFrame) SaveChunksToDisk() error {
	for i := 0; i < len(df.Columns[df.StructType.Field(0).Name]); i += df.rowChunkSize {
		if err := df.SaveRowChunkToDisk(i / df.rowChunkSize); err != nil {
			return err
		}
	}
	for i := 0; i < len(df.StructType.Field(0).Name); i += df.columnChunkSize {
		if err := df.SaveColumnChunkToDisk(i / df.columnChunkSize); err != nil {
			return err
		}
	}
	return nil
}

func (df *DataFrame) SaveRowChunkToDisk(chunkIndex int) error {
	// Ensure the directory exists
	if err := os.MkdirAll(df.filePath, os.ModePerm); err != nil {
		return err
	}

	filePath := filepath.Join(df.filePath, fmt.Sprintf("row_chunk_%d.gob", chunkIndex))
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	chunk := make(map[string][]interface{})
	for colName, colData := range df.Columns {
		start := chunkIndex * df.rowChunkSize
		end := start + df.rowChunkSize
		if end > len(colData) {
			end = len(colData)
		}
		chunk[colName] = colData[start:end]
	}

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(chunk); err != nil {
		return err
	}

	df.rowChunks[chunkIndex] = filePath
	return nil
}

func (df *DataFrame) SaveColumnChunkToDisk(chunkIndex int) error {
	// Ensure the directory exists
	if err := os.MkdirAll(df.filePath, os.ModePerm); err != nil {
		return err
	}

	filePath := filepath.Join(df.filePath, fmt.Sprintf("column_chunk_%d.gob", chunkIndex))
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	chunk := make(map[string][]interface{})
	for colName, colData := range df.Columns {
		if chunkIndex*df.columnChunkSize < len(colData) {
			chunk[colName] = colData[chunkIndex*df.columnChunkSize : (chunkIndex+1)*df.columnChunkSize]
		}
	}

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(chunk); err != nil {
		return err
	}

	df.columnChunks[chunkIndex] = filePath
	return nil
}

func (df *DataFrame) LoadRowChunkFromDisk(chunkIndex int) error {
	filePath, exists := df.rowChunks[chunkIndex]
	if (!exists) {
		return fmt.Errorf("row chunk %d not found", chunkIndex)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	chunk := make(map[string][]interface{})
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&chunk); err != nil {
		return err
	}

	for colName, colData := range chunk {
		df.Columns[colName] = append(df.Columns[colName][:chunkIndex*df.rowChunkSize], colData...)
	}

	return nil
}

func (df *DataFrame) LoadColumnChunkFromDisk(chunkIndex int) error {
	filePath, exists := df.columnChunks[chunkIndex]
	if !exists {
		return fmt.Errorf("column chunk %d not found", chunkIndex)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	chunk := make(map[string][]interface{})
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&chunk); err != nil {
		return err
	}

	for colName, colData := range chunk {
		df.Columns[colName] = append(df.Columns[colName][:chunkIndex*df.columnChunkSize], colData...)
	}

	return nil
}