package surfstore

import (
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
	"strings"
)

func ClientSync(client RPCClient) {

	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("Error when reading basedir")
	}
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("Could not load meta from meta file")
	}

	hashMap := make(map[string][]string)
	for _, file := range files {
		// check filename
		if file.Name() == "index.db" || strings.Contains(file.Name(), ",") || strings.Contains(file.Name(), "/") {
			continue
		}

		// get blocks numbers
		var blocksNum int = int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))
		fileToRead, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			log.Println("Error reading file in basedir: ", err)
		}

		defer fileToRead.Close()
		var hashset []string
		byteSlice := make([]byte, client.BlockSize)
		for i := 0; i < blocksNum; i++ {

			len, err := fileToRead.Read(byteSlice)
			if err != nil {
				log.Println("Error reading bytes from file in basedir: ", err)
			}

			hash := GetBlockHashString(byteSlice[:len])
			hashset = append(hashset, hash)
		}
		hashMap[file.Name()] = hashset
	}

	for filename, hashset := range hashMap {
		if localIndex[filename] == nil { // check new file, then update it
			localIndex[filename] = &FileMetaData{Filename: filename, Version: int32(1), BlockHashList: hashset}
		} else if !reflect.DeepEqual(localIndex[filename].BlockHashList, hashset) { // check changed file
			localIndex[filename].BlockHashList = hashset
			localIndex[filename].Version = localIndex[filename].Version + 1
		}
	}

	for fileName, fileMetaData := range localIndex {
		if _, ok := hashMap[fileName]; !ok { // update the feature of the deleted file
			if len(fileMetaData.BlockHashList) != 1 || fileMetaData.BlockHashList[0] != "0" {
				fileMetaData.Version++
				fileMetaData.BlockHashList = []string{"0"}
			}
		}
	}

	var bsAddrs []string
	if err := client.GetBlockStoreAddrs(&bsAddrs); err != nil {
		log.Println("Could not get blockStoreAddr: ", err)
	}

	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		log.Println("Could not get remote index: ", err)
	}

	for filename, localMD := range localIndex {
		m := make(map[string][]string)
		err = client.GetBlockStoreMap(hashMap[filename], &m) //return了一个blockStoreAdrr的map
		if err != nil {
			log.Println("Could not get blockStoreAddr: ", err)
		}
		if remoteMD, ok := remoteIndex[filename]; ok {
			if localMD.Version > remoteMD.Version {
				upload(client, localMD, bsAddrs, m)
			}
		} else {
			upload(client, localMD, bsAddrs, m)
		}
	}
	for filename, remoteMD := range remoteIndex {
		m := make(map[string][]string)
		err = client.GetBlockStoreMap(hashMap[filename], &m)
		if err != nil {
			log.Println("Could not get blockStoreAddr: ", err)
		}
		if localMD, ok := localIndex[filename]; !ok {
			localIndex[filename] = &FileMetaData{}
			download(client, localIndex[filename], remoteMD, m)
		} else {
			if remoteMD.Version >= localIndex[filename].Version {
				download(client, localMD, remoteMD, m)
			}
		}
	}

	WriteMetaFile(localIndex, client.BaseDir)
}

func upload(client RPCClient, FileMD *FileMetaData, bsAddrs []string, blockStoreMap map[string][]string) error {
	path := client.BaseDir + "/" + FileMD.Filename // local file path

	var latestVersion int32
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := client.UpdateFile(FileMD, &latestVersion)
		if err != nil {
			log.Println("Could not update file")
		} else {
			FileMD.Version = latestVersion
		}
		log.Println("Couldn't open Os")
		return err
	}

	file, err := os.Open(path)
	if err != nil {
		log.Println("Error opening file: ", err)
	}
	defer file.Close()

	fileStat, err := os.Stat(path)
	if err != nil {
		log.Println("Error geting fileInfo: ", err)
	}

	var numsBlocks int = int(math.Ceil(float64(fileStat.Size()) / float64(client.BlockSize)))
	for i := 0; i < numsBlocks; i++ {
		byteSlice := make([]byte, client.BlockSize)
		len, err := file.Read(byteSlice)
		if err != nil {
			log.Println("Error reading bytes from file in basedir: ", err)
		}
		byteSlice = byteSlice[:len] // read []byte from each block

		block := Block{BlockData: byteSlice, BlockSize: int32(len)}
		hash := GetBlockHashString(block.BlockData)

		var responsibleSever string
		for bsAddr, blockHashes := range blockStoreMap {
			for _, blockHash := range blockHashes {
				if hash == blockHash {
					responsibleSever = bsAddr
				}
			}
		}

		var succ bool
		if err := client.PutBlock(&block, strings.ReplaceAll(responsibleSever, "blockstore", ""), &succ); err != nil {
			log.Println("Could not put block")
		}
	}

	if err := client.UpdateFile(FileMD, &latestVersion); err != nil {
		log.Println("Could not update file")
	}

	FileMD.Version = latestVersion
	return nil
}

func download(client RPCClient, localMD *FileMetaData, remoteMD *FileMetaData, blockStoreMap map[string][]string) error {
	path := client.BaseDir + "/" + remoteMD.Filename // local file path

	file, err := os.Create(path)
	if err != nil {
		log.Println("Error creating file")
	}
	defer file.Close()

	_, err = os.Stat(path)
	if err != nil {
		log.Println("Error geting fileInfo")
	}

	if len(remoteMD.BlockHashList) == 1 && remoteMD.BlockHashList[0] == "0" {
		if err := os.Remove(path); err != nil {
			log.Println("Could not remove file")
		}
		*localMD = *remoteMD
		return nil
	}
	m := make(map[string][]string)
	err = client.GetBlockStoreMap(remoteMD.BlockHashList, &m)
	if err != nil {
		log.Println("Could not get blockStoreAddr: ", err)
	}

	Data := ""
	for _, hash := range remoteMD.BlockHashList {
		for bsAddr, blockHashes := range m {
			for _, blockHash := range blockHashes {

				if hash == blockHash {
					var block Block
					if err := client.GetBlock(hash, strings.ReplaceAll(bsAddr, "blockstore", ""), &block); err != nil {
						log.Println("Could not get block")
					}
					Data += string(block.BlockData)
				}
			}
		}
	}

	if _, err := file.WriteString(Data); err != nil {
		log.Println("Could not write to file:")
	}

	*localMD = *remoteMD
	return nil
}
