package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/qri-recipes/sitemap/sitemap"
	"github.com/spf13/cobra"
)

type Stats struct {
	UrlCount    int
	AvgNumLinks float32
	AvgPageSize float32

	FirstFetch time.Time
	LastFetch  time.Time

	HostUrlCount map[string]int
	StatusCount  map[int]int

	size  int64
	links int
}

var StatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "calculate stats on a sitemap",
	Run: func(cmd *cobra.Command, args []string) {
		path := "sitemap.json"
		data, err := ioutil.ReadFile(path)
		if err != nil {
			fmt.Printf("error reading sitemap file %s: %s", path, err.Error())
			return
		}

		urls := map[string]*sitemap.Url{}
		if err := json.Unmarshal(data, &urls); err != nil {
			fmt.Printf("error decoding JSON sitemap: %s", err.Error())
			return
		}

		stats := &Stats{
			UrlCount: len(urls),
		}

		defer func() {
			data, err := json.MarshalIndent(stats, "", "  ")
			if err != nil {
				fmt.Printf("error encoding stats to JSON: %s", err.Error())
				return
			}

			fmt.Println(string(data))
		}()

		if len(urls) == 0 {
			return
		}

		stats.HostUrlCount = map[string]int{}
		stats.StatusCount = map[int]int{}
		stats.FirstFetch = time.Date(3000, 0, 0, 0, 0, 0, 0, time.UTC)

		for rawurl, u := range urls {
			if u.Timestamp.Before(stats.FirstFetch) {
				stats.FirstFetch = u.Timestamp
			} else if u.Timestamp.After(stats.LastFetch) {
				stats.LastFetch = u.Timestamp
			}

			stats.links += len(u.Links)
			stats.size += u.ContentLength

			stats.StatusCount[u.Status]++
			if uu, err := url.Parse(rawurl); err == nil {
				stats.HostUrlCount[uu.Host]++
			}
		}

		stats.AvgNumLinks = float32(stats.links) / float32(len(urls))
		stats.AvgPageSize = float32(stats.size) / float32(len(urls))

	},
}
