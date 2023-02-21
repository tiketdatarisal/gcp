package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/tiketdatarisal/gcp/shared"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
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
func (s Storage) GetFileNames(bucketName string) (shared.StringSlice, error) {
	ctx, cancel := context.WithTimeout(s.ctx, timeoutDuration)
	defer cancel()

	fileIterator := s.client.Bucket(bucketName).Objects(ctx, nil)
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

// GetFileNamesWithPrefix return list of file names with prefix.
func (s Storage) GetFileNamesWithPrefix(bucketName, prefix string, restrictResult bool) (shared.StringSlice, error) {
	ctx, cancel := context.WithTimeout(s.ctx, timeoutDuration)
	defer cancel()

	var query *storage.Query = nil
	if prefix != "" {
		query = &storage.Query{Prefix: prefix}
		if restrictResult {
			query.Delimiter = `/`
		}
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

// FileMimeType return file mime type.
func (s Storage) FileMimeType(bucketName, fileName string) (string, error) {
	attr, err := s.client.Bucket(bucketName).Object(fileName).Attrs(s.ctx)
	if err != nil {
		return "", err
	}

	return attr.ContentType, nil
}

// IsFileExists return nil when file exists.
func (s Storage) IsFileExists(bucketName, fileName string) error {
	if _, err := s.FileMimeType(bucketName, fileName); err != nil {
		return err
	}

	return nil
}

// StreamReadFile streams a file for reading.
func (s Storage) StreamReadFile(bucketName, fileName string, ctx ...context.Context) (io.ReadCloser, error) {
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

// DownloadFile download a file into byte slice.
func (s Storage) DownloadFile(bucketName, fileName string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(s.ctx, timeoutDuration)
	defer cancel()

	reader, err := s.StreamReadFile(bucketName, fileName, ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrDownloadFailed, err)
	}

	return data, nil
}

// StreamWriteFile streams a file for writing.
func (s Storage) StreamWriteFile(bucketName, fileName string, ctx ...context.Context) io.WriteCloser {
	var writer *storage.Writer
	if len(ctx) > 0 {
		writer = s.client.Bucket(bucketName).Object(fileName).NewWriter(ctx[0])
	} else {
		writer = s.client.Bucket(bucketName).Object(fileName).NewWriter(s.ctx)
	}

	return writer
}

// UploadFile upload a file to a bucket.
func (s Storage) UploadFile(bucketName, fileName string, data []byte) error {
	ctx, cancel := context.WithTimeout(s.ctx, timeoutDuration)
	defer cancel()

	writer := s.StreamWriteFile(bucketName, fileName, ctx)
	defer func() { _ = writer.Close() }()

	_, err := writer.Write(data)
	if err != nil {
		return fmt.Errorf(errorWrapper, ErrUploadFailed, err)
	}

	return nil
}

// CopyFile copy a file from source to destination.
func (s Storage) CopyFile(srcBucket, srcFileName, dstBucket, dstFilename string) error {
	ctx, cancel := context.WithTimeout(s.ctx, timeoutDuration)
	defer cancel()

	srcObject := s.client.Bucket(srcBucket).Object(srcFileName)
	dstObject := s.client.Bucket(dstBucket).Object(dstFilename)

	if _, err := dstObject.CopierFrom(srcObject).Run(ctx); err != nil {
		return fmt.Errorf(errorWrapper, ErrCopyFailed, err)
	}

	return nil
}
