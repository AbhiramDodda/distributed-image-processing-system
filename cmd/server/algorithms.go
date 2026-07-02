package main

import (
	"io"
	"log/slog"
	"net/http"

	"github.com/abhiramd/petabyte-platform/internal/registry"
	"github.com/abhiramd/petabyte-platform/internal/sandbox"
)

// imageRegistry is where built algorithm images are pushed. In production this
// comes from config; kept as a constant until Level 6 multi-tenancy wires in
// per-tenant registries.
const imageRegistry = "registry.internal/petabyte"

// maxUploadBytes caps an algorithm package upload independent of the parser's
// uncompressed limit, so a client cannot stream an unbounded body.
const maxUploadBytes = 128 << 20 // 128 MiB

// algorithmRoutes mounts the Level 4 algorithm-registry API. Registration
// validates the package against tenant quota and records immutable metadata;
// the actual image build is performed out-of-band by the build workers keyed on
// the returned image reference, so this path needs no container runtime.
func algorithmRoutes(mux *http.ServeMux, reg *registry.Store, quota registry.Quota, log *slog.Logger) {
	mux.HandleFunc("/v1/algorithms", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			algos, err := reg.List(r.Context())
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, algos)
		case http.MethodPost:
			pkg, code, err := readPackage(r)
			if err != nil {
				writeError(w, code, err.Error())
				return
			}
			if err := registry.Validate(pkg.Manifest, quota); err != nil {
				writeError(w, http.StatusUnprocessableEntity, err.Error())
				return
			}
			owner := r.Header.Get("X-Tenant")
			if owner == "" {
				owner = "default"
			}
			algo := registry.Algorithm{
				Name: pkg.Manifest.Name,
				Version: pkg.Manifest.Version,
				Owner: owner,
				ImageRef: sandbox.ImageRef(imageRegistry, pkg),
				Digest: pkg.Digest,
				Manifest: pkg.Manifest,
			}
			if err := reg.Register(r.Context(), algo); err != nil {
				writeError(w, http.StatusConflict, err.Error())
				return
			}
			log.Info("algorithm registered", "name", algo.Name, "version", algo.Version, "owner", owner)
			writeJSON(w, http.StatusCreated, algo)
		default:
			writeError(w, http.StatusMethodNotAllowed, "GET or POST required")
		}
	})

	// Dry-run: parse + validate an uploaded package without registering it, so
	// users can check a submission against quota before committing a version.
	mux.HandleFunc("/v1/algorithms/validate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST required")
			return
		}
		pkg, code, err := readPackage(r)
		if err != nil {
			writeError(w, code, err.Error())
			return
		}
		if err := registry.Validate(pkg.Manifest, quota); err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"valid": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"valid": true,
			"manifest": pkg.Manifest,
			"image_ref": sandbox.ImageRef(imageRegistry, pkg),
		})
	})

	// /v1/algorithms/{name}/{version} — must be registered after the exact
	// "/v1/algorithms" and "/v1/algorithms/validate" patterns above.
	mux.HandleFunc("/v1/algorithms/", func(w http.ResponseWriter, r *http.Request) {
		parts := splitPath(r.URL.Path[len("/v1/algorithms/"):])
		switch {
		case len(parts) == 1:
			versions, err := reg.ListVersions(r.Context(), parts[0])
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, versions)
		case len(parts) == 2:
			algo, err := reg.Get(r.Context(), parts[0], parts[1])
			if err != nil {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, algo)
		default:
			writeError(w, http.StatusBadRequest, "use /v1/algorithms/{name}[/{version}]")
		}
	})
}

// readPackage reads a zip body and parses it. Returns an HTTP status alongside
// the error so callers map parse failures to 400 and everything else sensibly.
func readPackage(r *http.Request) (*sandbox.Package, int, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxUploadBytes))
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	pkg, err := sandbox.ParsePackage(body)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	return pkg, http.StatusOK, nil
}
