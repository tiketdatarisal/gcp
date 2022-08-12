package storage

import "context"

// Singleton return instance of Storage.
func Singleton(credentialFile ...string) *Storage {
	Error = nil
	mutex.Lock()
	defer mutex.Unlock()

	if s != nil {
		return s
	}

	st, err := NewStorage(context.Background(), credentialFile...)
	if err != nil {
		Error = err
		return nil
	}

	s = st
	return st
}

// Close client connection to Google Storage API.
func Close() {
	if s != nil {
		s.Close()
	}
}
