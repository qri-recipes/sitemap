package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"

	"github.com/datatogether/ffi"
	"github.com/spf13/cobra"
)

var unfetchedFilePath = "unfetched.txt"

type Url struct {
	Url   string
	Links []string
}

var UnfetchedUrlsCmd = &cobra.Command{
	Use:   "unfetched",
	Short: "list sitemap destination links that haven't been fetched",
	Run: func(cmd *cobra.Command, args []string) {
		data, err := ioutil.ReadFile("sitemap.json")
		if err != nil {
			panic(err.Error())
		}

		urls := map[string]Url{}
		if err := json.Unmarshal(data, &urls); err != nil {
			panic(err.Error())
		}

		f, err := os.OpenFile(unfetchedFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}

		unfetched := 0
		checked := 0
		for _, u := range urls {
			for _, l := range u.Links {
				checked++
				link, err := url.Parse(l)
				if err != nil {
					panic("error parsing url: " + err.Error())
				}

				if link.Host != "epa.gov" {
					continue
				}

				if err := isWebpageURL(link); err == nil {
					if _, ok := urls[l]; !ok {
						unfetched++
						if _, err := f.Write(append([]byte(l), '\n')); err != nil {
							panic(err)
						}
					}
				}

			}
		}
		f.Close()

		fmt.Printf(`wrote %d/%d unfetched links to %s`, unfetched, checked, unfetchedFilePath)
	},
}

var htmlMimeTypes = map[string]bool{
	"text/html":                 true,
	"text/html; charset=utf-8":  true,
	"text/plain; charset=utf-8": true,
	"text/xml; charset=utf-8":   true,
}

// htmlExtensions is a dictionary of "file extensions" that normally contain
// html content
var htmlExtensions = map[string]bool{
	".asp":   true,
	".aspx":  true,
	".cfm":   true,
	".html":  true,
	".net":   true,
	".php":   true,
	".xhtml": true,
	".":      true,
	"":       true,
}

var invalidSchemes = map[string]bool{
	"data":   true,
	"mailto": true,
	"ftp":    true,
}

func isWebpageURL(u *url.URL) error {
	if invalidSchemes[u.Scheme] {
		return fmt.Errorf("invalid scheme: %s", u.Scheme)
	}

	filename, err := ffi.FilenameFromUrlString(u.String())
	if err != nil {
		return fmt.Errorf("ffi err: %s", err.Error())
	}

	ext := filepath.Ext(filename)
	if !htmlExtensions[ext] {
		return fmt.Errorf("non-webpage extension: %s", ext)
	}

	return nil
}
