package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/tiketdatarisal/gcp/shared"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"io/ioutil"
)

type Storage struct {
	ctx    context.Context
	client *storage.Client
}

// NewStorage return a new Storage client.
func NewStorage(ctx context.Context, credentialFile ...string) (*Storage, error) {
	var err error
	var client *storage.Client
	if len(credentialFile) > 0 {
		client, err = storage.NewClient(ctx, option.WithCredentialsFile(credentialFile[0]))
	} else {
		client, err = storage.NewClient(ctx)
	}

	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrInitStorageClientFailed, err)
	}

	return &Storage{
		ctx:    ctx,
		client: client,
	}, nil
}

// Close closes the Storage client.
func (s Storage) Close() {
	if s.client != nil {
		_ = s.client.Close()
	}
}

// GetBucketNames returns a list of bucket names.
func (s Storage) GetBucketNames(projectID string) (shared.StringSlice, error) {
	ctx, cancel := context.WithTimeout(s.ctx, timeoutDuration)
	defer cancel()

	bucketIterator := s.client.Buckets(ctx, projectID)
	var bucketNames shared.StringSlice
	for {
		bucket, err := bucketIterator.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, fmt.Errorf(errorWrapper, ErrGetBucketNamesFailed, err)
		}

		bucketNames = append(bucketNames, bucket.Name)
	}

	return bucketNames, nil
}

// GetFileNames return list of file names.
func (s Storage) GetFileNames(bucketName string, prefix ...string) (shared.StringSlice, error) {
	ctx, cancel := context.WithTimeout(s.ctx, timeoutDuration)
	defer cancel()

	var query *storage.Query = nil
	if len(prefix) > 0 {
		query = &storage.Query{Prefix: prefix[0]}
	}

	fileIterator := s.client.Bucket(bucketName).Objects(ctx, query)
	var fileNames shared.StringSlice
	for {
		file, err := fileIterator.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, fmt.Errorf(errorWrapper, ErrGetFilenamesFailed, err)
		}

		fileNames = append(fileNames, file.Name)
	}

	return fileNames, nil
}

func (s Storage) IsFileExists(bucketName, fileName string) bool {
	if _, err := s.client.Bucket(bucketName).Object(fileName).Attrs(s.ctx); err != nil {
		return false
	}

	return true
}

// StreamFile streams a file.
func (s Storage) StreamFile(bucketName, fileName string, ctx ...context.Context) (io.ReadCloser, error) {
	var reader *storage.Reader
	var err error
	if len(ctx) > 0 {
		reader, err = s.client.Bucket(bucketName).Object(fileName).NewReader(ctx[0])
	} else {
		reader, err = s.client.Bucket(bucketName).Object(fileName).NewReader(s.ctx)
	}

	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrStreamFailed, err)
	}

	return reader, nil
}

// DownloadFile downloads a file into byte slice.
func (s Storage) DownloadFile(bucketName, fileName string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(s.ctx, timeoutDuration)
	defer cancel()

	reader, err := s.StreamFile(bucketName, fileName, ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrDownloadFailed, err)
	}

	return data, nil
}
