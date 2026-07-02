package sandbox

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
)

// Files every algorithm package must contain. Users submit a zip; the platform
// refuses anything missing one of these so a build never fails halfway.
const (
	fileDockerfile = "Dockerfile"
	fileEntrypoint = "main.py"
	fileRequirements = "requirements.txt"
	fileManifest = "manifest.json"
)

// maxPackageBytes caps the uncompressed size the platform will accept from a
// single package, guarding against zip-bomb style submissions.
const maxPackageBytes = 256 << 20 // 256 MiB

// Package is a parsed, shape-validated algorithm submission. Digest is the
// SHA-256 of the original zip bytes and doubles as the immutable version key
// for the registry: two uploads with identical contents map to one image.
type Package struct {
	Manifest Manifest
	Files map[string][]byte
	Digest string
}

// ParsePackage reads an algorithm zip, verifies the required files are present,
// decodes manifest.json, and checks manifest shape. It does NOT apply tenant
// quota — that is the registry validator's job — so this stays reusable by the
// standalone package-lint path.
func ParsePackage(archive []byte) (*Package, error) {
	zr, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return nil, fmt.Errorf("open package zip: %w", err)
	}

	files := make(map[string][]byte)
	var total int64
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		// Reject path traversal (../ or absolute) so extraction can never
		// escape the build directory.
		name := path.Clean(f.Name)
		if path.IsAbs(name) || name == ".." || len(name) >= 3 && name[:3] == "../" {
			return nil, fmt.Errorf("package: unsafe path %q", f.Name)
		}
		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", name, err)
		}
		data, err := io.ReadAll(io.LimitReader(rc, maxPackageBytes-total+1))
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		total += int64(len(data))
		if total > maxPackageBytes {
			return nil, fmt.Errorf("package exceeds %d bytes uncompressed", maxPackageBytes)
		}
		files[name] = data
	}

	for _, required := range []string{fileDockerfile, fileEntrypoint, fileRequirements, fileManifest} {
		if _, ok := files[required]; !ok {
			return nil, fmt.Errorf("package missing required file %q", required)
		}
	}

	var m Manifest
	if err := json.Unmarshal(files[fileManifest], &m); err != nil {
		return nil, fmt.Errorf("decode manifest.json: %w", err)
	}
	if err := m.checkShape(); err != nil {
		return nil, err
	}

	return &Package{
		Manifest: m,
		Files: files,
		Digest: contentDigest(files),
	}, nil
}

// contentDigest hashes the package contents canonically (files in sorted order,
// each length-prefixed) rather than the raw zip bytes. Zip archives embed
// timestamps and vary with entry ordering, so hashing them would give a
// different digest for byte-identical algorithm code — defeating build dedupe.
func contentDigest(files map[string][]byte) string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)

	h := sha256.New()
	for _, name := range names {
		fmt.Fprintf(h, "%s\x00%d\x00", name, len(files[name]))
		h.Write(files[name])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
