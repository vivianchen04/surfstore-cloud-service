package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) { // * means the pointer and creat new object, return the reference
	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return nil, fmt.Errorf("GetBlock wrong")
	} else {
		return &Block{BlockData: block.GetBlockData(), BlockSize: block.GetBlockSize()}, nil // & means we get the reference, and write something on the reference
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	h := GetBlockHashString(block.BlockData)
	bs.BlockMap[h] = block
	return &Success{Flag: true}, nil
}

// Given an input hashlist,
// returns an output hashlist containing the subset of hashlist_in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	hashes := []string{}
	for _, hash := range blockHashesIn.Hashes {
		blc, _ := bs.GetBlock(ctx, &BlockHash{Hash: hash})
		if blc != nil {
			hashes = append(hashes, hash)
		}
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockMap := bs.BlockMap
	hashes := []string{}
	for key := range blockMap {
		hashes = append(hashes, key)
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
