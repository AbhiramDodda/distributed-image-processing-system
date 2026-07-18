package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Client struct {
	s3 *s3.Client
	bucket string
}

type ClientConfig struct {
	Endpoint string
	Region string
	Bucket string
	AccessKeyID string
	SecretAccessKey string
	UsePathStyle bool
}

func NewClient(ctx context.Context, cfg ClientConfig) (*Client, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey, "",
		)),
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	s3opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3opts = append(s3opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.UsePathStyle
		})
	}
	return &Client{
		s3: s3.NewFromConfig(awsCfg, s3opts...),
		bucket: cfg.Bucket,
	}, nil
}

func (c *Client) Put(ctx context.Context, key string, r io.Reader, contentType string) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key: aws.String(key),
		Body: r,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

func (c *Client) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key: aws.String(key),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("get %s: %w", key, err)
	}
	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return out.Body, size, nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key: aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

func (c *Client) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	var token *string
	for {
		out, err := c.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(c.bucket),
			Prefix: aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list prefix %s: %w", prefix, err)
		}
		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

// Copy performs a server-side copy of src to dst within the bucket; no object
// data flows through this process. It is idempotent in dst — copying different
// sources to the same dst leaves exactly one object — which is what makes it
// usable as the commit point of the two-phase result protocol (see
// scheduler.Committer).
func (c *Client) Copy(ctx context.Context, src, dst string) error {
	_, err := c.s3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: aws.String(c.bucket),
		CopySource: aws.String(fmt.Sprintf("%s/%s", c.bucket, src)),
		Key: aws.String(dst),
	})
	if err != nil {
		return fmt.Errorf("copy %s -> %s: %w", src, dst, err)
	}
	return nil
}

func (c *Client) CopyStorageClass(ctx context.Context, key, storageClass string) error {
	_, err := c.s3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: aws.String(c.bucket),
		CopySource: aws.String(fmt.Sprintf("%s/%s", c.bucket, key)),
		Key: aws.String(key),
		StorageClass: s3StorageClass(storageClass),
	})
	if err != nil {
		return fmt.Errorf("copy storage class for %s: %w", key, err)
	}
	return nil
}

func (c *Client) S3() *s3.Client { return c.s3 }
func (c *Client) Bucket() string { return c.bucket }

func s3StorageClass(class string) s3types.StorageClass {
	switch class {
	case "STANDARD_IA":
		return s3types.StorageClassStandardIa
	case "GLACIER_IR":
		return s3types.StorageClassGlacierIr
	case "DEEP_ARCHIVE":
		return s3types.StorageClassDeepArchive
	default:
		return s3types.StorageClassStandard
	}
}
