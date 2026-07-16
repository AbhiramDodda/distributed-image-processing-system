// Command gen-images writes N synthetic random-noise JPEGs into a directory,
// for exercising the ingestion pipeline and job scheduler without a real dataset.
//
// Noise compresses poorly, so each file lands in a realistic size range (a
// 512x512 image at quality 90 is ~235 KB), which makes it easy to hit a target
// total size: ~44,000 images ≈ 10 GB. Filenames are <class>_<n>.jpg so the
// hash-prefix sharding spreads them across all 256 shards.
//
//	go run ./cmd/gen-images -out ~/petabyte-demo/images -count 44000
package main

import (
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
)

func main() {
	out := flag.String("out", "./images", "output directory")
	count := flag.Int("count", 100, "number of images to generate")
	dim := flag.Int("dim", 512, "image width/height in pixels")
	quality := flag.Int("quality", 90, "JPEG quality (1-100)")
	shard := flag.String("shard", "", "if set (a 2-hex-digit prefix like \"7a\"), emit only filenames that hash to this one shard, concentrating every image into a single shard for the work-stealing demo")
	flag.Parse()

	if err := run(*out, *count, *dim, *quality, *shard); err != nil {
		log.Fatal(err)
	}
}

func run(out string, count, dim, quality int, shard string) error {
	if err := os.MkdirAll(out, 0o755); err != nil {
		return fmt.Errorf("create out dir: %w", err)
	}

	classes := []string{"cat", "dog", "bird", "car", "tree"}
	// nameFor is the single source of truth for an image's filename, so the
	// shard-targeting producer can pre-test a candidate's shard with the exact
	// name the consumer will write. ShardKey hashes the base name, matching how
	// the ingestion pipeline assigns objects to shards.
	nameFor := func(i int) string { return fmt.Sprintf("%s_%06d.jpg", classes[i%len(classes)], i) }
	var done int64
	start := time.Now()

	// One goroutine per core; each owns its own RNG to avoid contention.
	workers := runtime.NumCPU()
	jobs := make(chan int, workers*2)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := range jobs {
				name := filepath.Join(out, nameFor(i))
				if err := writeNoiseJPEG(name, dim, quality, rng); err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
				if n := atomic.AddInt64(&done, 1); n%2000 == 0 {
					fmt.Printf("  generated %d/%d (%.0f img/s)\n", n, count, float64(n)/time.Since(start).Seconds())
				}
			}
		}(int64(w) + 1)
	}
	if shard == "" {
		for i := 0; i < count; i++ {
			jobs <- i
		}
	} else {
		// Mine the counter for names that hash into the target shard. Roughly 1 in
		// 256 candidates match, so this scans ~256*count names -- cheap SHA-256 work.
		for i, emitted := 0, 0; emitted < count; i++ {
			if storage.ShardKey(nameFor(i)) == shard {
				jobs <- i
				emitted++
			}
		}
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errs:
		return err
	default:
	}
	fmt.Printf("done: %d images in %s\n", count, time.Since(start).Round(time.Millisecond))
	return nil
}

func writeNoiseJPEG(path string, dim, quality int, rng *rand.Rand) error {
	img := image.NewRGBA(image.Rect(0, 0, dim, dim))
	for y := 0; y < dim; y++ {
		for x := 0; x < dim; x++ {
			img.Set(x, y, color.RGBA{
				R: uint8(rng.Intn(256)),
				G: uint8(rng.Intn(256)),
				B: uint8(rng.Intn(256)),
				A: 255,
			})
		}
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := jpeg.Encode(f, img, &jpeg.Options{Quality: quality}); err != nil {
		return err
	}
	return f.Close()
}
