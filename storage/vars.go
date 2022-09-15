package storage

import (
	"errors"
	"sync"
	"time"
)

const (
	timeoutDuration = 30 * time.Second
	errorWrapper    = "%w: %v"
)

var (
	s     *Storage
	mutex sync.Mutex
	Error error

	ErrInitStorageClientFailed = errors.New("could not initialize Storage client")
	ErrGetBucketNamesFailed    = errors.New("could not get Storage bucket names")
	ErrGetFilenamesFailed      = errors.New("could not get Storage file names")
	ErrStreamFailed            = errors.New("could not stream from Storage service")
	ErrDownloadFailed          = errors.New("could not download from Storage service")
	ErrUploadFailed            = errors.New("could not upload to Storage service")
	ErrCopyFailed              = errors.New("could not copy file")
)
