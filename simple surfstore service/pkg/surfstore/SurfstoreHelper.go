package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	for _, data := range fileMetas {
		for hashIndex, hashValue := range data.BlockHashList {
			filename := data.Filename
			version := data.Version
			statement, _ = db.Prepare(insertTuple)
			statement.Exec(filename, version, hashIndex, hashValue)
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct fileName from indexes;`

const getTuplesByFileName string = `select version, hashValue from indexes where fileName=? order by hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fmt.Println(metaFilePath)
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if errors.Is(e, os.ErrNotExist) {
		fmt.Println("file is not exist")
		f, e := os.Create(metaFilePath)
		if e != nil {
			return nil, e
		}
		f.Close()
		db, err := sql.Open("sqlite3", metaFilePath)
		if err != nil {
			fmt.Println("error when open database")
		}
		statement, err := db.Prepare(createTable)
		if err != nil {
			fmt.Println("Error when create table")
		}
		statement.Exec()
	} else if metaFileStats.IsDir() {
		return fileMetaMap, fmt.Errorf("meta file path is a dir")
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		fmt.Println("Error When Opening Meta")
	}
	defer db.Close()

	filenames, err := db.Query(getDistinctFileName)
	if err != nil {
		fmt.Println(err)
	}
	// defer filenames.Close()

	for filenames.Next() {
		var fileName string
		var version int
		var hashValues []string
		err := filenames.Scan(&fileName)
		if err != nil {
			log.Println("Error of scanning rows in db", err)
		}
		tuples, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			log.Println("Error of scanning rows in db", err)
		}
		for tuples.Next() {
			var hashValue string
			err := tuples.Scan(&version, &hashValue)
			if err != nil {
				log.Println("Error of scanning rows in db", err)
			}
			hashValues = append(hashValues, hashValue)
		}
		fileMetaMap[fileName] = &FileMetaData{Filename: fileName, Version: int32(version), BlockHashList: hashValues}
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
