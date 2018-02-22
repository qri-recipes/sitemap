package main

import (
	"testing"
)

func TestCrawl(t *testing.T) {
	stop := make(chan bool)
	crawl := NewCrawl()

	urls, err := crawl.Start(stop)
	if err != nil {
		t.Error(err)
		return
	}

	if err := writeJSON("sitemap.json", urls); err != nil {
		t.Error(err)
		return
	}
}
