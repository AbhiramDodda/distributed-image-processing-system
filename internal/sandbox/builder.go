package sandbox

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// ImageRef returns the fully-qualified OCI reference for an algorithm version.
// The package digest is used as the tag so identical uploads dedupe to one
// image and a name+version can never silently change contents.
func ImageRef(registry string, p *Package) string {
	return fmt.Sprintf("%s/%s:%s", registry, p.Manifest.Name, p.Digest[:12])
}

// BuildResult is what a successful build yields: the pushed reference and the
// registry-assigned content digest.
type BuildResult struct {
	ImageRef string
	Digest string
}

// Builder abstracts image construction so the worker/registry code is testable
// without docker. DockerBuilder is the production implementation.
type Builder interface {
	Build(ctx context.Context, p *Package, registry string) (*BuildResult, error)
}

// DockerBuilder writes a package to a temp context dir and shells out to
// `docker build`/`docker push`. Subsequent runs of the same digest are a no-op
// at the registry layer, so the platform caches builds by digest upstream.
type DockerBuilder struct {
	DockerBin string
}

func (b DockerBuilder) bin() string {
	if b.DockerBin != "" {
		return b.DockerBin
	}
	return "docker"
}

// writeContext materialises the package files into a build directory. Exposed
// (lowercase, same package) so tests can assert the layout without building.
func writeContext(p *Package, dir string) error {
	for name, data := range p.Files {
		full := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", name, err)
		}
		if err := os.WriteFile(full, data, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}
	return nil
}

func (b DockerBuilder) Build(ctx context.Context, p *Package, registry string) (*BuildResult, error) {
	dir, err := os.MkdirTemp("", "petabyte-build-*")
	if err != nil {
		return nil, fmt.Errorf("build context: %w", err)
	}
	defer os.RemoveAll(dir)

	if err := writeContext(p, dir); err != nil {
		return nil, err
	}

	ref := ImageRef(registry, p)
	build := exec.CommandContext(ctx, b.bin(), "build", "-t", ref, dir)
	if out, err := build.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("docker build %s: %w\n%s", ref, err, out)
	}
	push := exec.CommandContext(ctx, b.bin(), "push", ref)
	if out, err := push.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("docker push %s: %w\n%s", ref, err, out)
	}
	return &BuildResult{ImageRef: ref, Digest: p.Digest}, nil
}
