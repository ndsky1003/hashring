package hashring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// hash_ring 结构体
type hash_ring[T any] struct {
	rwl          sync.RWMutex //protect under
	hashMap      map[uint64]T
	sortedHashes []uint64
	opt          *Option[T]
}

// New 创建新的 HashRing
func New[T any](opts ...*Option[T]) *hash_ring[T] {
	opt := Options[T]().
		SetReplicaCount(3).
		SetStringFunc(func(t T) string { return fmt.Sprintf("%v", t) }).
		merges(opts...)

	return &hash_ring[T]{
		hashMap: make(map[uint64]T),
		opt:     opt,
	}
}

func (this *hash_ring[T]) hash(key string) uint64 {
	hash := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(hash[:8])
}

// AddNode 添加节点
func (this *hash_ring[T]) AddNode(node T) {
	this.rwl.Lock()
	defer this.rwl.Unlock()

	for i := 0; i < *this.opt.replica_count; i++ {
		virtualNode := fmt.Sprintf("%s#%d", this.opt.string_func(node), i)
		hash := this.hash(virtualNode)
		this.hashMap[hash] = node
		this.sortedHashes = append(this.sortedHashes, hash)
	}
	sort.Slice(this.sortedHashes, func(i, j int) bool {
		return this.sortedHashes[i] < this.sortedHashes[j]
	})
}

// RemoveNode 移除节点
func (this *hash_ring[T]) RemoveNode(node T) {
	this.rwl.Lock()
	defer this.rwl.Unlock()

	for i := 0; i < *this.opt.replica_count; i++ {
		virtualNode := fmt.Sprintf("%s#%d", this.opt.string_func(node), i)
		hash := this.hash(virtualNode)
		delete(this.hashMap, hash)

		for j, h := range this.sortedHashes {
			if h == hash {
				this.sortedHashes = append(this.sortedHashes[:j], this.sortedHashes[j+1:]...)
				break
			}
		}
	}

	sort.Slice(this.sortedHashes, func(i, j int) bool {
		return this.sortedHashes[i] < this.sortedHashes[j]
	})
}

// GetNode 获取对应的节点
func (this *hash_ring[T]) GetNode(key string) T {
	this.rwl.RLock()
	defer this.rwl.RUnlock()

	if len(this.sortedHashes) == 0 {
		return this.hashMap[0] //0值即可
	}

	hash := this.hash(key)
	index := sort.Search(len(this.sortedHashes), func(i int) bool {
		return this.sortedHashes[i] >= hash
	})

	if index == len(this.sortedHashes) {
		index = 0
	}

	return this.hashMap[this.sortedHashes[index]]
}
