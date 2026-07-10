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
)

func main() {
	out := flag.String("out", "./images", "output directory")
	count := flag.Int("count", 100, "number of images to generate")
	dim := flag.Int("dim", 512, "image width/height in pixels")
	quality := flag.Int("quality", 90, "JPEG quality (1-100)")
	flag.Parse()

	if err := run(*out, *count, *dim, *quality); err != nil {
		log.Fatal(err)
	}
}

func run(out string, count, dim, quality int) error {
	if err := os.MkdirAll(out, 0o755); err != nil {
		return fmt.Errorf("create out dir: %w", err)
	}

	classes := []string{"cat", "dog", "bird", "car", "tree"}
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
				name := filepath.Join(out, fmt.Sprintf("%s_%06d.jpg", classes[i%len(classes)], i))
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
	for i := 0; i < count; i++ {
		jobs <- i
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
