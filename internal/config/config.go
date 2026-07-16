package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Storage StorageConfig `yaml:"storage"`
	Metadata MetadataConfig `yaml:"metadata"`
	Coordinator CoordinatorConfig `yaml:"coordinator"`
	Worker WorkerConfig `yaml:"worker"`
	Operator OperatorConfig `yaml:"operator"`
	Server ServerConfig `yaml:"server"`
	Ingestion IngestionConfig `yaml:"ingestion"`
	Tiering TieringConfig `yaml:"tiering"`
	Metrics MetricsConfig `yaml:"metrics"`
	Logging LoggingConfig `yaml:"logging"`
}

type StorageConfig struct {
	Endpoint string `yaml:"endpoint"`
	Region string `yaml:"region"`
	Bucket string `yaml:"bucket"`
	AccessKeyID string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	UsePathStyle bool `yaml:"use_path_style"`
	MultipartThreshold int64 `yaml:"multipart_threshold"`
	MultipartChunkSize int64 `yaml:"multipart_chunk_size"`
	MultipartConcurrency int `yaml:"multipart_concurrency"`
}

type MetadataConfig struct {
	DBPath string `yaml:"db_path"`
}

// RaftConfig configures this coordinator's membership in the Raft cluster that
// replicates the commit ledger. ID is this node's Raft id; Port is its raft gRPC
// listener; Peers lists every member (including self) with the address of its
// raft listener.
type RaftConfig struct {
	ID uint64 `yaml:"id"`
	Port int `yaml:"port"`
	Peers []RaftPeer `yaml:"peers"`
}

type RaftPeer struct {
	ID uint64 `yaml:"id"`
	Address string `yaml:"address"`
}

// Enabled reports whether raft consensus is configured for this coordinator.
func (r RaftConfig) Enabled() bool { return r.ID > 0 && len(r.Peers) > 0 }

type CoordinatorConfig struct {
	Host string `yaml:"host"`
	Port int `yaml:"port"`
	SuspectTimeout time.Duration `yaml:"suspect_timeout"`
	DeadTimeout time.Duration `yaml:"dead_timeout"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	DispatchInterval time.Duration `yaml:"dispatch_interval"`
	VnodesPerNode int `yaml:"vnodes_per_node"`
	TaskMaxRetries int `yaml:"task_max_retries"`
	// LeaseChunk is the number of shard items granted per lease renewal. It bounds
	// how far a worker runs ahead of its last progress report, and so sets the
	// steal granularity: a smaller chunk means a whole shard becomes splittable
	// sooner and leaves a larger un-granted tail for an idle worker to reclaim.
	// Zero uses the scheduler default (1000).
	LeaseChunk int64 `yaml:"lease_chunk"`
	WALDir string `yaml:"wal_dir"`
	CheckpointInterval time.Duration `yaml:"checkpoint_interval"`
	// Backpressure: global ceiling on concurrently in-flight jobs. Submissions
	// past it are shed (HTTP 429). Disabled (unbounded) unless > 0.
	MaxInFlightJobs int `yaml:"max_in_flight_jobs"`
	// Raft consensus for multi-process HA + consensus-agreed exactly-once commits.
	// Disabled unless Raft.ID > 0 and Raft.Peers is set.
	Raft RaftConfig `yaml:"raft"`
	// gRPC control-plane API (Level 6). Disabled unless GRPCPort > 0. When
	// enabled, JWTSecret must be set -- the server refuses to start with auth
	// unconfigured rather than serving an open API.
	GRPCPort int `yaml:"grpc_port"`
	JWTSecret string `yaml:"jwt_secret"`
	RateLimitPerSec float64 `yaml:"rate_limit_per_sec"`
	RateLimitBurst int `yaml:"rate_limit_burst"`
}

type WorkerConfig struct {
	ID string `yaml:"id"`
	CoordinatorURL string `yaml:"coordinator_url"`
	Host string `yaml:"host"`
	Port int `yaml:"port"`
	Concurrency int `yaml:"concurrency"`
	PollInterval time.Duration `yaml:"poll_interval"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
}

type ServerConfig struct {
	Host string `yaml:"host"`
	Port int `yaml:"port"`
	MetadataDBPath string `yaml:"metadata_db_path"`
	RegistryDBPath string `yaml:"registry_db_path"`
}

type IngestionConfig struct {
	Workers int `yaml:"workers"`
	BatchSize int `yaml:"batch_size"`
	MaxFileSizeMB int64 `yaml:"max_file_size_mb"`
}

type TieringConfig struct {
	HotDaysThreshold int `yaml:"hot_days_threshold"`
	WarmDaysThreshold int `yaml:"warm_days_threshold"`
	ColdDaysThreshold int `yaml:"cold_days_threshold"`
}

type MetricsConfig struct {
	Enabled bool `yaml:"enabled"`
	Port int `yaml:"port"`
	Path string `yaml:"path"`
}

type OperatorConfig struct {
	Namespace string `yaml:"namespace"`
	WorkerImage string `yaml:"worker_image"`
	CoordinatorURL string `yaml:"coordinator_url"`
	PollInterval time.Duration `yaml:"poll_interval"`
	MaxJobsPerCycle int `yaml:"max_jobs_per_cycle"`
	RayDashboardURL string `yaml:"ray_dashboard_url"`
	KubeconfigPath string `yaml:"kubeconfig_path"`
}

type LoggingConfig struct {
	Level string `yaml:"level"`
	Format string `yaml:"format"`
}

func DefaultConfig() *Config {
	return &Config{
		Storage: StorageConfig{
			Endpoint:             "http://localhost:9000",
			Region:               "us-east-1",
			Bucket:               "petabyte-images",
			AccessKeyID:          "minioadmin",
			SecretAccessKey:      "minioadmin",
			UsePathStyle:         true,
			MultipartThreshold:   100 * 1024 * 1024,
			MultipartChunkSize:   64 * 1024 * 1024,
			MultipartConcurrency: 8,
		},
		Metadata: MetadataConfig{
			DBPath: "./metadata.db",
		},
		Coordinator: CoordinatorConfig{
			Host:              "0.0.0.0",
			Port:              8090,
			SuspectTimeout:    10 * time.Second,
			DeadTimeout:       20 * time.Second,
			HeartbeatInterval: 5 * time.Second,
			DispatchInterval:  1 * time.Second,
			VnodesPerNode:     150,
			TaskMaxRetries:    3,
			WALDir:            "./coordinator-wal",
			CheckpointInterval: 30 * time.Second,
			GRPCPort:          0,
			RateLimitPerSec:   50,
			RateLimitBurst:    100,
		},
		Worker: WorkerConfig{
			CoordinatorURL:    "http://localhost:8090",
			Host:              "0.0.0.0",
			Port:              9001,
			Concurrency:       4,
			PollInterval:      2 * time.Second,
			HeartbeatInterval: 5 * time.Second,
		},
		Server: ServerConfig{
			Host:           "0.0.0.0",
			Port:           8080,
			MetadataDBPath: "./metadata.db",
			RegistryDBPath: "./registry.db",
		},
		Ingestion: IngestionConfig{
			Workers:       16,
			BatchSize:     100,
			MaxFileSizeMB: 500,
		},
		Tiering: TieringConfig{
			HotDaysThreshold:  30,
			WarmDaysThreshold: 90,
			ColdDaysThreshold: 365,
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    9090,
			Path:    "/metrics",
		},
		Operator: OperatorConfig{
			Namespace:       "petabyte",
			WorkerImage:     "petabyte-worker:latest",
			CoordinatorURL:  "http://coordinator:8090",
			PollInterval:    5 * time.Second,
			MaxJobsPerCycle: 10,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
		},
	}
}

func Load(path string) (*Config, error) {
	cfg := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}
