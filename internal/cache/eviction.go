package cache

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// admit records a freshly stored object with the eviction policy and reclaims
// objects until total usage is back under the byte budget. If the same id was
// admitted before (a concurrent re-fetch), its size delta is reconciled rather
// than double-counted. Caller must hold c.mu.
func (c *Cache) admit(id, path string, size int64) {
	if e, ok := c.objects[id]; ok {
		c.curBytes += size - e.size
		e.size = size
		e.path = path
		c.policy.Access(id)
	} else {
		c.objects[id] = &entry{id: id, path: path, size: size}
		c.policy.Add(id)
		c.curBytes += size
	}
	c.evictLocked()
}

// evictLocked removes objects chosen by the eviction policy until usage fits the
// budget. A single object larger than maxBytes is kept (evicting it would not
// help and the caller still needs it); everything behind it is dropped. Caller
// holds mu.
func (c *Cache) evictLocked() {
	for c.curBytes > c.maxBytes && c.policy.Len() > 1 {
		id, ok := c.policy.Victim()
		if !ok {
			return
		}
		e := c.objects[id]
		c.policy.Remove(id)
		delete(c.objects, id)
		c.curBytes -= e.size
		if err := os.Remove(e.path); err != nil && !os.IsNotExist(err) {
			c.log.Warn("cache: evict remove failed", "id", id, "err", err)
		} else {
			c.log.Debug("cache: evicted", "id", id, "bytes", e.size)
		}
	}
}

// adopt rebuilds the in-memory index from objects left on disk by a previous
// run. LRU order is seeded from file modification time (oldest at the tail), and
// any objects beyond the budget are evicted immediately. Leftover temp files
// from an interrupted write are cleaned up. Caller holds no locks (New only).
func (c *Cache) adopt() error {
	ents, err := os.ReadDir(c.root)
	if err != nil {
		return err
	}
	type found struct {
		path string
		size int64
		mtime int64
	}
	var objs []found
	for _, de := range ents {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		full := filepath.Join(c.root, name)
		if strings.HasPrefix(name, ".tmp-") {
			os.Remove(full)
			continue
		}
		info, err := de.Info()
		if err != nil {
			continue
		}
		objs = append(objs, found{path: full, size: info.Size(), mtime: info.ModTime().UnixNano()})
	}
	sort.Slice(objs, func(i, j int) bool { return objs[i].mtime < objs[j].mtime })
	for _, o := range objs {
		// The filename is idFor(originalKey); the original key is not recoverable
		// from disk, but the id is exactly what Get looks up, so hits still work.
		// Adding oldest-first leaves the newest object most-recently-used under LRU.
		id := filepath.Base(o.path)
		c.objects[id] = &entry{id: id, path: o.path, size: o.size}
		c.policy.Add(id)
		c.curBytes += o.size
	}
	c.evictLocked()
	return nil
}
