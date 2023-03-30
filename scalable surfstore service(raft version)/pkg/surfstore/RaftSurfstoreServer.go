package surfstore

import (
	context "context"
	"fmt"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	// Added for discussion
	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

// If the node is the leader, and if a majority of the nodes are working, should return the correct answer;
// if a majority of the nodes are crashed, should block until a majority recover.
// If not the leader, should indicate an error back to the client
func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	// TODO if a majority of the nodes are crashed, should block until a majority recover
	for {
		majorAlive, _ := s.SendHeartbeat(ctx, empty)
		if majorAlive.Flag {
			break
		}
	}
	return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	// TODO if a majority of the nodes are crashed, should block until a majority recover
	for {
		majorAlive, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if majorAlive.Flag {
			break
		}
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	// TODO if a majority of the nodes are crashed, should block until a majority recover
	for {
		majorAlive, _ := s.SendHeartbeat(ctx, empty)
		if majorAlive.Flag {
			break
		}
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		s.lastApplied = s.commitIndex
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, ERR_SERVER_CRASHED
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	// send entry to all my followers and count the replies
	responses := make(chan bool, len(s.peers)-1)
	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		go s.sendToFollower(ctx, addr, responses)
	}

	totalAppends := 1
	// wait in loop for responses
	for {
		s.isCrashedMutex.RLock()
		isCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()
		if isCrashed {
			*s.pendingCommits[int64(len(s.pendingCommits)-1)] <- false
			return
		}
		result := <-responses
		if result {
			totalAppends++
		}
		if totalAppends > len(s.peers)/2 {
			// TODO put on correct channel
			*s.pendingCommits[len(s.pendingCommits)-1] <- true
			// TODO update commit Index correctly
			s.commitIndex++
			break
		}
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, responses chan bool) {
	for {
		s.isCrashedMutex.RLock()
		isCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()
		if isCrashed {
			responses <- false
		}

		var prevLogTerm int64
		if s.commitIndex == -1 {
			prevLogTerm = 0
		} else {
			prevLogTerm = s.log[s.commitIndex].Term
		}
		dummyAppendEntriesInput := AppendEntryInput{
			Term: s.term,
			// TODO put the right values
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: s.commitIndex,
			// Entries:      s.log[:s.commitIndex+2],
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		// TODO check all errors
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		appendEntryOutput, _ := client.AppendEntries(ctx, &dummyAppendEntriesInput)

		// TODO check output
		if appendEntryOutput != nil && appendEntryOutput.Success {
			responses <- true
			return
		}
	}
}

// 1. Reply false if term < currentTerm (Â§5.1)
// 2. Reply false if log doesnâ€™t contain an entry at prevLogIndex whose term
// matches prevLogTerm (Â§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (Â§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
	}

	output := &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}

	// 1. Reply false if term < currentTerm (Â§5.1)
	if input.Term < s.term {
		return output, fmt.Errorf("leader out of date")
	}
	// prevLogIndex := input.PrevLogIndex
	// if prevLogIndex != -1 {
	//  log := s.log[prevLogIndex]
	//  // 2. Reply false if log doesnâ€™t contain an entry at prevLogIndex whose term
	//  // matches prevLogTerm (Â§5.3)
	//  if log == nil || log.Term != input.PrevLogTerm {
	//   return &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: prevLogIndex}, fmt.Errorf("log doesnâ€™t contain an entry at prevLogIndex")
	//  }
	// }
	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (Â§5.3)
	for index, logEntry := range s.log {
		s.lastApplied = int64(index - 1)
		// delete extra entries
		if len(input.Entries) < index+1 {
			s.log = s.log[:index]
			input.Entries = make([]*UpdateOperation, 0)
			break
		}
		// delete the existing entry and all that follow it
		if logEntry != input.Entries[index] {
			s.log = s.log[:index]
			input.Entries = input.Entries[index:]
			break
		}
		// append more entries
		if len(s.log) == index+1 { //last iteration, all match
			input.Entries = input.Entries[index+1:]
		}
	}
	// 4. Append any new entries not already in the log

	// TODO actually check entries
	s.log = append(s.log, input.Entries...)
	// 5. If leaderCommit > s.commitIndex, set commitIndex = min(leaderCommit, index
	// of last new entry)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))

		for s.lastApplied < s.commitIndex {
			s.lastApplied++
			entry := s.log[s.lastApplied]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
	}
	output.Success = true
	// output.MatchedIndex =
	return output, nil
}

// Should return ERR_SERVER_CRASHED error; procedure has no effect if server is crashed
// Success:
// True if the server was successfully made the leader
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++
	// TODO update state
	return &Success{Flag: true}, nil
}

// Should return ERR_SERVER_CRASHED error; procedure has no effect if server is crashed
// We will not check the output of SendHeartbeat. You can return True always or have some custom logic based on your implementation.
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	// contact all the follower, send some AppendEntries call
	majorAlive := false
	aliveCount := 1
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return &Success{Flag: false}, nil
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryInput from s.nextIndex, etc
		var prevLogTerm int64
		if s.commitIndex == -1 {
			prevLogTerm = 0
		} else {
			prevLogTerm = s.log[s.commitIndex].Term
		}
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: s.commitIndex,
			// TODO figure out which entries to send
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, input)
		if output != nil {
			aliveCount++
			if aliveCount > len(s.peers)/2 {
				majorAlive = true
			}
		}
	}
	return &Success{Flag: majorAlive}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
