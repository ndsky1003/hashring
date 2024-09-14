package hashring

import (
	"crypto/md5"
	"fmt"
	"sort"
	"sync"
)

// hash_ring 结构体
type hash_ring[T any] struct {
	mu            sync.RWMutex //protect under
	hashMap       map[uint32]T
	sortedHashes  []uint32
	replica_count int
	string_func   func(T) string
}

// New 创建新的 HashRing
func New[T any](opts ...*options[T]) *hash_ring[T] {
	opt := Options[T]().merges(opts...)
	if opt.replica_count == nil || *opt.replica_count == 0 {
		panic("replica_count must be > 0")
	}

	if opt.string_func == nil {
		opt.string_func = func(t T) string { return fmt.Sprintf("%v", t) }
	}

	return &hash_ring[T]{
		hashMap:       make(map[uint32]T),
		replica_count: *opt.replica_count,
		string_func:   opt.string_func,
	}
}

func (this *hash_ring[T]) hash(key string) uint32 {
	hash := md5.Sum([]byte(key))
	v := uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
	return v
}

// AddNode 添加节点
func (this *hash_ring[T]) AddNode(node T) {
	this.mu.Lock()
	defer this.mu.Unlock()

	for i := 0; i < this.replica_count; i++ {
		virtualNode := fmt.Sprintf("%s#%d", this.string_func(node), i)
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
	this.mu.Lock()
	defer this.mu.Unlock()

	for i := 0; i < this.replica_count; i++ {
		virtualNode := fmt.Sprintf("%s#%d", this.string_func(node), i)
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
	this.mu.RLock()
	defer this.mu.RUnlock()

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
