package pipeline

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// WAL is an append-only, crash-safe write-ahead log of opaque records with
// periodic snapshotting. Coordinator/scheduler state changes are appended as
// records; a Checkpoint folds current state into a snapshot and truncates the
// log. On restart the latest snapshot is loaded and only the records appended
// after it are replayed, reconstructing in-memory state with no database.
//
// Every record carries a monotonic sequence number. A Checkpoint records the
// high-water sequence it covers, so if the process crashes after the snapshot
// is written but before the log is truncated, recovery still replays only
// records newer than the snapshot — never double-applying.
//
// On-disk framing of every record (log and snapshot alike):
//
//	uint32 bodyLen | uint32 crc32(body) | body
//	body = uint64 seq | payload
//
// A torn tail (a frame not fully flushed before a crash) is detected on
// recovery via a short read or CRC mismatch and truncated away; the log resumes
// from the last intact frame.
type WAL struct {
	mu sync.Mutex
	dir string
	logPath string
	snapPath string
	log *os.File
	seq uint64
	logger *slog.Logger
}

// Recovery is the state reconstructed from disk when a WAL is opened.
type Recovery struct {
	Snapshot []byte // payload of the most recent checkpoint, nil if none
	Records [][]byte // record payloads appended after that checkpoint, in order
}

const (
	walLogName = "wal.log"
	walSnapName = "wal.snap"
	frameHeaderLen = 8 // uint32 bodyLen + uint32 crc
)

var crcTable = crc32.MakeTable(crc32.IEEE)

// OpenWAL opens (creating if needed) the WAL rooted at dir and returns the
// recovered state. The returned WAL is positioned for appends.
func OpenWAL(dir string, logger *slog.Logger) (*WAL, *Recovery, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("wal: create dir %s: %w", dir, err)
	}
	w := &WAL{
		dir: dir,
		logPath: filepath.Join(dir, walLogName),
		snapPath: filepath.Join(dir, walSnapName),
		logger: logger,
	}

	snapshot, snapSeq, err := w.readSnapshot()
	if err != nil {
		return nil, nil, err
	}
	records, maxSeq, err := w.recoverLog(snapSeq)
	if err != nil {
		return nil, nil, err
	}
	w.seq = max(snapSeq, maxSeq)

	f, err := os.OpenFile(w.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("wal: open log for append: %w", err)
	}
	w.log = f
	return w, &Recovery{Snapshot: snapshot, Records: records}, nil
}

// Append durably writes one record. It assigns the next sequence number, frames
// the record, and fsyncs before returning, so a successful Append survives a
// crash.
func (w *WAL) Append(payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq++
	if _, err := w.log.Write(frame(w.seq, payload)); err != nil {
		return fmt.Errorf("wal: append seq %d: %w", w.seq, err)
	}
	if err := w.log.Sync(); err != nil {
		return fmt.Errorf("wal: fsync append seq %d: %w", w.seq, err)
	}
	return nil
}

// Checkpoint atomically records snapshot as the state covering every record
// appended so far, then truncates the log. Callers pass a serialization of the
// full in-memory state; subsequent recovery loads it instead of replaying the
// compacted records.
func (w *WAL) Checkpoint(snapshot []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	hw := w.seq
	tmp := w.snapPath + ".tmp"
	if err := os.WriteFile(tmp, frame(hw, snapshot), 0o644); err != nil {
		return fmt.Errorf("wal: write snapshot temp: %w", err)
	}
	if err := fsyncPath(tmp); err != nil {
		return fmt.Errorf("wal: fsync snapshot temp: %w", err)
	}
	if err := os.Rename(tmp, w.snapPath); err != nil {
		return fmt.Errorf("wal: commit snapshot: %w", err)
	}
	if err := fsyncPath(w.dir); err != nil {
		return fmt.Errorf("wal: fsync dir after snapshot: %w", err)
	}

	if err := w.log.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate log: %w", err)
	}
	if _, err := w.log.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: rewind log: %w", err)
	}
	if err := w.log.Sync(); err != nil {
		return fmt.Errorf("wal: fsync truncated log: %w", err)
	}
	w.logger.Info("wal: checkpoint written", "high_water_seq", hw, "bytes", len(snapshot))
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.log == nil {
		return nil
	}
	err := w.log.Close()
	w.log = nil
	return err
}

// readSnapshot loads the checkpoint file if present. A missing file is not an
// error (fresh WAL); a corrupt one is, since it is written atomically and must
// be intact if it exists.
func (w *WAL) readSnapshot() ([]byte, uint64, error) {
	data, err := os.ReadFile(w.snapPath)
	if os.IsNotExist(err) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("wal: read snapshot: %w", err)
	}
	seq, payload, _, err := readFrame(data)
	if err != nil {
		return nil, 0, fmt.Errorf("wal: corrupt snapshot: %w", err)
	}
	return payload, seq, nil
}

// recoverLog scans the log, returning payloads of records newer than snapSeq and
// the maximum sequence observed. A torn or corrupt tail is truncated away.
func (w *WAL) recoverLog(snapSeq uint64) ([][]byte, uint64, error) {
	data, err := os.ReadFile(w.logPath)
	if os.IsNotExist(err) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("wal: read log: %w", err)
	}

	var records [][]byte
	var maxSeq uint64
	var off int
	for off < len(data) {
		seq, payload, n, ferr := readFrame(data[off:])
		if ferr != nil {
			w.logger.Warn("wal: truncating torn tail", "at_offset", off, "reason", ferr)
			if terr := os.Truncate(w.logPath, int64(off)); terr != nil {
				return nil, 0, fmt.Errorf("wal: truncate torn tail: %w", terr)
			}
			break
		}
		off += n
		if seq > maxSeq {
			maxSeq = seq
		}
		if seq > snapSeq {
			records = append(records, payload)
		}
	}
	return records, maxSeq, nil
}

// frame encodes one sequence-numbered record.
func frame(seq uint64, payload []byte) []byte {
	body := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint64(body[:8], seq)
	copy(body[8:], payload)

	buf := make([]byte, frameHeaderLen+len(body))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(body)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.Checksum(body, crcTable))
	copy(buf[frameHeaderLen:], body)
	return buf
}

// readFrame decodes one frame at the start of data, returning its sequence,
// payload, and total bytes consumed. A short read or CRC mismatch yields an
// error, which the caller treats as a torn tail.
func readFrame(data []byte) (seq uint64, payload []byte, n int, err error) {
	if len(data) < frameHeaderLen {
		return 0, nil, 0, io.ErrUnexpectedEOF
	}
	bodyLen := binary.BigEndian.Uint32(data[:4])
	want := binary.BigEndian.Uint32(data[4:8])
	end := frameHeaderLen + int(bodyLen)
	if bodyLen < 8 || end > len(data) {
		return 0, nil, 0, io.ErrUnexpectedEOF
	}
	body := data[frameHeaderLen:end]
	if crc32.Checksum(body, crcTable) != want {
		return 0, nil, 0, fmt.Errorf("crc mismatch")
	}
	seq = binary.BigEndian.Uint64(body[:8])
	payload = make([]byte, len(body)-8)
	copy(payload, body[8:])
	return seq, payload, end, nil
}

// fsyncPath opens the named file or directory and fsyncs it, forcing the
// preceding write or rename to stable storage.
func fsyncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
