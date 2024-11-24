package hashring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
)

// hash_ring 结构体
type hash_ring[T any] struct {
	hashMap *treemap.Map
	opt     *Option[T]
}

// New 创建新的 HashRing
func New[T any](opts ...*Option[T]) *hash_ring[T] {
	opt := Options[T]().
		SetReplicaCount(3).
		SetStringFunc(func(t T) string { return fmt.Sprintf("%v", t) }).
		merges(opts...)

	return &hash_ring[T]{
		hashMap: treemap.NewWith(utils.UInt64Comparator),
		opt:     opt,
	}
}

func (this *hash_ring[T]) hash(key string) uint64 {
	hash := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(hash[:8])
}

// AddNode 添加节点
func (this *hash_ring[T]) AddNode(node T, opts ...*Option[T]) {
	opt := Options[T]().merge(this.opt).merges(opts...)
	for i := 0; i < *opt.replica_count; i++ {
		virtualNode := fmt.Sprintf("%s#%d", this.opt.string_func(node), i)
		hash := this.hash(virtualNode)
		this.hashMap.Put(hash, node)
	}
}

// RemoveNode 移除节点
func (this *hash_ring[T]) RemoveNode(node T, opts ...*Option[T]) {
	opt := Options[T]().merge(this.opt).merges(opts...)
	for i := 0; i < *opt.replica_count; i++ {
		virtualNode := fmt.Sprintf("%s#%d", this.opt.string_func(node), i)
		hash := this.hash(virtualNode)
		this.hashMap.Remove(hash)
	}
}

// GetNode 获取对应的节点
func (this *hash_ring[T]) GetNode(key string) (T, error) {
	hash := this.hash(key)
	node_key, node := this.hashMap.Ceiling(hash)
	if node_key == nil {
		node_key, node = this.hashMap.Ceiling(uint64(0))
	}
	if node_key != nil {
		return node.(T), nil
	}
	var zero T
	return zero, fmt.Errorf("node not found")
}
