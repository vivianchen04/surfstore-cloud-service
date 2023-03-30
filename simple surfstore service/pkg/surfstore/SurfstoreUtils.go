package surfstore

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {

	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("Error for reading files in BaseDir", err)
	}

	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("Error for loading meta from meta file", err)
	}

	hashMap := make(map[string][]string)
	for _, file := range files {
		if file.Name() == "index.db" {
			continue
		}
		var blocksNum int = int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))
		fileToRead, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			log.Println("Error for opening file in basedir", err)
		}

		for i := 0; i < blocksNum; i++ {
			// we want to read the file content into blocks and get the hashvalue and hashindex
			byteSlice := make([]byte, client.BlockSize)
			len, err := fileToRead.Read(byteSlice)
			if err != nil {
				log.Println("Error for reading bytes from a file in basedir", err)
			}
			byteSlice = byteSlice[:len]
			hash := GetBlockHashString(byteSlice)
			hashMap[file.Name()] = append(hashMap[file.Name()], hash)
		}

		if v, ok := localIndex[file.Name()]; ok {
			if !reflect.DeepEqual(hashMap[file.Name()], v.BlockHashList) {
				localIndex[file.Name()].BlockHashList = hashMap[file.Name()]
				localIndex[file.Name()].Version++
			}
		} else {
			meta := FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: hashMap[file.Name()]}
			localIndex[file.Name()] = &meta
		}
	}
	// delete block validation
	for name, data := range localIndex {
		if _, ok := hashMap[name]; !ok {
			if len(data.BlockHashList) != 1 || data.BlockHashList[0] != "0" {
				data.Version++
				data.BlockHashList = []string{"0"}
			}
		}
	}

	// Next, the client should connect to the server and download an updated FileInfoMap.
	// For the purposes of this discussion, let’s call this the “remote index.”
	var bsAddr string
	if err := client.GetBlockStoreAddr(&bsAddr); err != nil {
		log.Println("Erro of getting blockstore address ", err)
	}

	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		log.Println("Error of getting remote Index ", err)
	}

	// upload
	for fileName, localmd := range localIndex {
		if remotemd, ok := remoteIndex[fileName]; ok {
			if localmd.Version > remotemd.Version {
				upload(client, localmd, bsAddr)
			}
		} else {
			// if remoteIndex doesn't have the metaData
			upload(client, localmd, bsAddr)
		}
	}

	// dowload
	for filename, remotemd := range remoteIndex {
		if localmd, ok := localIndex[filename]; ok {
			if localmd.Version < remotemd.Version {
				download(client, localmd, remotemd, bsAddr)
			} else if localmd.Version == remotemd.Version && !reflect.DeepEqual(localmd.BlockHashList, remotemd.BlockHashList) {
				download(client, localmd, remotemd, bsAddr)
			}
		} else {
			// if local doesnt have the remote data
			localIndex[filename] = &FileMetaData{}
			localfile := localIndex[filename]
			download(client, localfile, remotemd, bsAddr)
		}
	}
	WriteMetaFile(localIndex, client.BaseDir)
}

func download(client RPCClient, localMD *FileMetaData, remoteMD *FileMetaData, bsAddr string) error {
	path := client.BaseDir + "/" + remoteMD.Filename
	file, err := os.Create(path)
	if err != nil {
		log.Println("Error creating file: ", err)
	}
	defer file.Close()
	*localMD = *remoteMD

	if len(remoteMD.BlockHashList) == 1 && remoteMD.BlockHashList[0] == "0" {
		if err := os.Remove(path); err != nil {
			log.Println("Could not remove local file: ", err)
			return err
		}
		return nil
	}

	data := ""
	for _, hash := range remoteMD.BlockHashList {
		var block Block
		if err := client.GetBlock(hash, bsAddr, &block); err != nil {
			log.Println("Failed to get block: ", err)
		}
		data += string(block.BlockData)
	}
	file.WriteString(data)
	return nil
}

func upload(client RPCClient, MD *FileMetaData, bsAddr string) error {
	path := client.BaseDir + "/" + MD.Filename
	var latestVersion int32
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err = client.UpdateFile(MD, &latestVersion)
		if err != nil {
			log.Println("Could not upload file: ", err)
		}
		MD.Version = latestVersion
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		log.Println("Error opening file: ", err)
	}
	defer file.Close()

	fileStat, _ := os.Stat(path)
	var blocksNum int = int(math.Ceil(float64(fileStat.Size()) / float64(client.BlockSize)))
	for i := 0; i < blocksNum; i++ {
		byteSlice := make([]byte, client.BlockSize)
		len, err := file.Read(byteSlice)
		if err != nil && err != io.EOF {
			log.Println("Error reading bytes from file in basedir: ", err)
		}
		byteSlice = byteSlice[:len]

		block := Block{BlockData: byteSlice, BlockSize: int32(len)}

		var succ bool
		if err := client.PutBlock(&block, bsAddr, &succ); err != nil {
			log.Println("Failed to put block: ", err)
		}
	}

	if err := client.UpdateFile(MD, &latestVersion); err != nil {
		log.Println("Failed to update file: ", err)
		MD.Version = -1
	}
	MD.Version = latestVersion

	return nil
}
