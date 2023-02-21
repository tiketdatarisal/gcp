package config

import "time"

const (
	RunQueryConfigRetry              = 3
	RunQueryConfigCompressed         = false
	RunQueryConfigDelimiterComma     = ","
	RunQueryConfigDelimiterSemicolon = ";"
	RunQueryConfigDisableHeader      = false
	RunQueryConfigDelay              = 500 * time.Millisecond
	RunQueryConfigTimeout            = 0
)

// RunQueryConfig is a config for RunQueryXXX functions.
// When not initialized will be used default values.
type RunQueryConfig struct {
	// Labels set labels that will be used when run a query job (Optional).
	Labels Labels

	// Retry number of retries (Optional). Have default value of 3.
	Retry int

	// Compressed represent whether the query result stored will be compressed or not (Optional).
	Compressed bool

	// Delimiter represent delimiter used when exporting data to CSV (Optional). Have default value of (,) comma.
	Delimiter string

	// DisableHeader represent whether exported data will omit the header (Optional).
	DisableHeader bool

	// Delay duration taken before query job will be retried (Optional). Have default value of 500 ms.
	Delay time.Duration

	// Timeout max duration before one query job will be cancelled (Optional). Have default value of 0 (have no timeout).
	Timeout time.Duration
}

// RunQueryConfigDefault is an instance of default RunQueryConfig.
// You can use this config as reference for your own config.
var RunQueryConfigDefault = RunQueryConfig{
	Labels:        nil,
	Retry:         RunQueryConfigRetry,
	Compressed:    RunQueryConfigCompressed,
	Delimiter:     RunQueryConfigDelimiterComma,
	DisableHeader: RunQueryConfigDisableHeader,
	Delay:         RunQueryConfigDelay,
	Timeout:       RunQueryConfigTimeout,
}

// InitRunQueryConfig return an initialized RunQueryConfig with filled-in default values.
func InitRunQueryConfig(config ...RunQueryConfig) RunQueryConfig {
	if len(config) == 0 {
		return RunQueryConfigDefault
	}

	c := config[0]
	if c.Retry < 0 {
		c.Retry = RunQueryConfigRetry
	}

	if c.Delimiter == "" {
		c.Delimiter = RunQueryConfigDelimiterComma
	}

	if c.Delay < 0 {
		c.Delay = RunQueryConfigDelay
	}

	if c.Timeout < 0 {
		c.Timeout = RunQueryConfigTimeout
	}

	return c
}
