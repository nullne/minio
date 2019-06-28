package volume

import (
	"sync"
	"sync/atomic"
)

type pathTrie struct {
	sync.RWMutex
	count    *int32
	children map[string]*pathTrie
}

func newPathTrie() *pathTrie {
	return &pathTrie{
		children: make(map[string]*pathTrie),
		count:    new(int32),
	}
}

func (t *pathTrie) getChild(key string) (*pathTrie, bool) {
	t.RLock()
	defer t.RUnlock()
	child, ok := t.children[key]
	return child, ok
}

func (t *pathTrie) list(segments []string, max int) []string {
	node := t
	for _, s := range segments {
		child, ok := node.getChild(s)
		if !ok {
			return nil
		}
		node = child
	}

	node.RLock()
	entries := make([]string, 0, len(node.children))
	for k := range node.children {
		entries = append(entries, k)
		max--
		if max == 0 {
			break
		}
	}
	node.RUnlock()
	return entries
}

// recursive may be better
func (t *pathTrie) delete(segments []string) {
	node := t
	for _, s := range segments {
		child, ok := node.getChild(s)
		if !ok {
			return
		}
		if res := atomic.AddInt32(child.count, -1); res > 0 {
			node = child
			continue
		}
		// delete the node
		node.Lock()
		delete(node.children, s)
		node.Unlock()
		return
	}
}

func (t *pathTrie) put(segments []string) {
	node := t
	for _, s := range segments {
		child, ok := node.getChild(s)
		if !ok {
			child = newPathTrie()
			node.Lock()
			node.children[s] = child
			node.Unlock()
		}
		atomic.AddInt32(child.count, 1)
		node = child
	}
}
