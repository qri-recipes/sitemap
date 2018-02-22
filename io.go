package main

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func (c *Crawl) loadSitemapFile(path string) error {
	if filepath.Ext(path) == ".json" {
		if f, err := os.Open(path); err == nil {
			log.Infof("loading previous sitemap: %s", path)
			urls := make(map[string]*Url)
			if err := json.NewDecoder(f).Decode(&urls); err != nil {
				return nil
			}
			c.urlLock.Lock()
			defer c.urlLock.Unlock()

			added := 0
			for urlstr, u := range urls {
				if u.Status == 200 {
					c.urls[urlstr] = u
					added++
				}
			}
			log.Info("********************")
			log.Infof("added: %d prior urls", added)
			log.Info("********************")
		}
	}
	return nil
}

func (c *Crawl) writeJSON(path string) error {
	log.Infof("writing json index file to path: %s", path)

	c.urlLock.Lock()
	defer func() {
		log.Infof("done writing json index file: %s", path)
		c.urlLock.Unlock()
	}()

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	us := make(map[string]*Url)
	i := 0
	for key, u := range c.urls {
		if !u.Timestamp.IsZero() {
			us[key] = u
			i++
		}
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(us)
}
