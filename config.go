package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// Config is the global configuration for the crawler
type Config struct {
	// UserAgent is the string identifiers for crawler requests
	UserAgent string
	// Domains is the list of domains to crawl. Only domains listed
	// in the
	Domains []string
	// Seeds is a list of urls to seed the crawler with
	Seeds []string
	// Parallelism
	Parallelism int
	// CrawlDelayMilliseconds determines how long to wait between fetches
	// for a given crawler
	CrawlDelayMilliseconds int
	// StopAfterEntries kills the crawler after a specified number of urls have been visited
	// default of 0 don't limit the number of entries
	StopAfterEntries int
	// Polite is weather or not to respect robots.txt
	Polite bool
	// SrcPath is the path to an input site file from a previous crawl
	SrcPath string
	// DestPath is the path to the output site file
	DestPath string
	// RecordResponseHeaders sets weather or not to keep a map of response headers
	RecordResponseHeaders bool
	// StaleDuration
	StaleDurationHours int
	// BatchWriteInterval configures how often to stop & write a batch backup
	BatchWriteInterval int
}

// DefaultConfig returns the default configuration
func DefaultConfig() Config {
	return Config{
		Domains:                []string{"https://datatogether.org"},
		Seeds:                  []string{"https://datatogether.org"},
		Parallelism:            2,
		CrawlDelayMilliseconds: 1000,
		DestPath:               "sitemap.json",
		StopAfterEntries:       5,
		RecordResponseHeaders:  false,
		Polite:                 true,
	}
}

// JSONConfigFromFilepath returns a func that reads a json-encoded
// config if the file specified by filepath exists
func JSONConfigFromFilepath(path string) func(*Config) {
	return func(c *Config) {
		if data, err := ioutil.ReadFile(path); err == nil {
			cfg := Config{}
			log.Infof("using config file: %s", path)
			if err := json.Unmarshal(data, &cfg); err != nil {
				log.Errorf("error parsing configuration file at path: %s: %s", path, err.Error())
				os.Exit(1)
			}
			*c = cfg
		}
	}
}
