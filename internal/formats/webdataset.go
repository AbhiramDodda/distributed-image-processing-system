package formats

import (
	"archive/tar"
	"fmt"
	"io"
	"sort"
)

// Component is one file of a WebDataset sample: an extension (no leading dot,
// e.g. "jpg", "cls", "json") and its bytes.
type Component struct {
	Ext string
	Data []byte
}

// ShardWriter writes a WebDataset shard: a plain tar archive where every file of
// a sample shares a basename (the sample key) and differs only by extension,
// e.g. img000017.jpg + img000017.cls. PyTorch's WebDataset loader groups
// consecutive tar entries by key into one sample, so a sample's components must
// be contiguous — WriteSample guarantees that and rejects duplicate keys.
type ShardWriter struct {
	tw *tar.Writer
	samples int
	seen map[string]struct{}
}

func NewShardWriter(w io.Writer) *ShardWriter {
	return &ShardWriter{tw: tar.NewWriter(w), seen: make(map[string]struct{})}
}

// WriteSample writes all components of one sample contiguously. Components are
// emitted in a deterministic (extension-sorted) order so identical inputs
// produce byte-identical shards.
func (s *ShardWriter) WriteSample(key string, comps ...Component) error {
	if key == "" {
		return fmt.Errorf("webdataset: empty sample key")
	}
	if len(comps) == 0 {
		return fmt.Errorf("webdataset: sample %q has no components", key)
	}
	if _, dup := s.seen[key]; dup {
		return fmt.Errorf("webdataset: duplicate sample key %q", key)
	}

	sorted := make([]Component, len(comps))
	copy(sorted, comps)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Ext < sorted[j].Ext })

	for _, c := range sorted {
		if c.Ext == "" {
			return fmt.Errorf("webdataset: sample %q has a component with no extension", key)
		}
		name := key + "." + c.Ext
		hdr := &tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(c.Data)),
			Typeflag: tar.TypeReg,
		}
		if err := s.tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("webdataset: header %s: %w", name, err)
		}
		if _, err := s.tw.Write(c.Data); err != nil {
			return fmt.Errorf("webdataset: write %s: %w", name, err)
		}
	}
	s.seen[key] = struct{}{}
	s.samples++
	return nil
}

// Samples returns how many samples have been written.
func (s *ShardWriter) Samples() int { return s.samples }

// Close finalizes the tar archive. It must be called before the shard is used.
func (s *ShardWriter) Close() error {
	if err := s.tw.Close(); err != nil {
		return fmt.Errorf("webdataset: close tar: %w", err)
	}
	return nil
}
