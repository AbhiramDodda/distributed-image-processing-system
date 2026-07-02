package sandbox

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// resultFile is the conventional name the algorithm writes into /output. The
// platform reads only this structured summary from the sandbox; everything else
// in the output volume is treated as opaque result artifacts to be uploaded.
const resultFile = "result.json"

// AlgorithmOutput is the structured summary an algorithm emits. Counts flow
// back into the scheduler's TaskResult so job progress and billing are driven
// by what the sandbox actually processed, not by what the platform guessed.
type AlgorithmOutput struct {
	ImagesProcessed int64 `json:"images_processed"`
	ImagesFailed int64 `json:"images_failed"`
	// Artifacts are output-relative paths the worker should upload to the
	// results bucket (e.g. embeddings, prediction files).
	Artifacts []string `json:"artifacts"`
	Metrics map[string]float64 `json:"metrics,omitempty"`
}

// CollectOutput reads and validates result.json from an execution's output
// directory. A missing or malformed file is an algorithm contract violation,
// not a platform error, so it surfaces as an explicit error the worker records
// against the task.
func CollectOutput(outputDir string) (*AlgorithmOutput, error) {
	data, err := os.ReadFile(filepath.Join(outputDir, resultFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("algorithm produced no %s", resultFile)
		}
		return nil, fmt.Errorf("read %s: %w", resultFile, err)
	}
	var out AlgorithmOutput
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode %s: %w", resultFile, err)
	}

	// Artifact paths must stay inside the output volume; reject traversal so a
	// compromised algorithm cannot trick the worker into uploading host files.
	for _, a := range out.Artifacts {
		clean := filepath.Clean(a)
		if filepath.IsAbs(clean) || clean == ".." || len(clean) >= 3 && clean[:3] == ".."+string(filepath.Separator) {
			return nil, fmt.Errorf("artifact path escapes output volume: %q", a)
		}
	}
	return &out, nil
}
