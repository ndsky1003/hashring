package hashring

import (
	"fmt"
	"testing"
)

// TestNewHashRing 测试 New 函数
func TestNewHashRing(t *testing.T) {
	ring := New[string]()
	if ring == nil {
		t.Errorf("Expected non-nil hash_ring, got nil")
	}
}

// TestAddNode 测试 AddNode 函数
func TestAddNode(t *testing.T) {
	ring := New[string]()
	ring.AddNode("node1")

	if ring.hashMap.Size() != *ring.opt.replica_count {
		t.Errorf("Expected %d nodes, got %d", *ring.opt.replica_count, ring.hashMap.Size())
	}
}

// TestRemoveNode 测试 RemoveNode 函数
func TestRemoveNode(t *testing.T) {
	ring := New[string]()
	ring.AddNode("node1")
	ring.RemoveNode("node1")

	if ring.hashMap.Size() != 0 {
		t.Errorf("Expected 0 nodes, got %d", ring.hashMap.Size())
	}
}

// TestGetNode 测试 GetNode 函数
func TestGetNode(t *testing.T) {
	ring := New[string]()
	ring.AddNode("node1")
	ring.AddNode("node2", Options[string]().SetReplicaCount(12))

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("ke1y%d", i)
		node, err := ring.GetNode(key)
		if err != nil {
			t.Errorf("Expected node, got error %v", err)
		}
		t.Log(key, node)
		if node != "node1" && node != "node2" {
			t.Errorf("Unexpected node, got %v", node)
		}
	}
	t.Log("==========================")
	ring.RemoveNode("node2", Options[string]().SetReplicaCount(12))
	ring.RemoveNode("node1", Options[string]().SetReplicaCount(12))
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("ke1y%d", i)
		node, err := ring.GetNode(key)
		if err != nil {
			t.Errorf("Expected node, got error %v", err)
		}
		t.Log(key, node)
		if node != "node1" && node != "node2" {
			t.Errorf("Unexpected node, got %v", node)
		}
	}

}

// Main 测试入口
func TestMain(m *testing.M) {
	m.Run()
}
