package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"

	"github.com/qri-recipes/sitemap/sitemap"
	"github.com/spf13/cobra"
)

var InboundLinksCmd = &cobra.Command{
	Use:   "inbound-links",
	Short: "list links to a given url",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		var (
			u   *url.URL
			err error
		)

		path := args[0]
		rawurl := args[1]
		data, err := ioutil.ReadFile(path)
		if err != nil {
			fmt.Printf("error reading sitemap file %s: %s", path, err.Error())
			return
		}

		u, err = url.Parse(rawurl)
		if err != nil {
			fmt.Errorf("error parsing url:\n\t%s\n\t%s", rawurl, err.Error())
			return
		}
		urlstr := sitemap.NormalizeURLString(u)

		urls := map[string]*sitemap.Url{}
		if err := json.Unmarshal(data, &urls); err != nil {
			fmt.Printf("error decoding JSON sitemap: %s", err.Error())
			return
		}

		inbound := []string{}

		checked := 0
		found := 0
		for ustr, urlinfo := range urls {
			checked++
			for _, l := range urlinfo.Links {
				if urlstr == l {
					found++
					inbound = append(inbound, ustr)
					break
				}
			}
		}

		linkdata, err := json.Marshal(inbound)
		if err != nil {
			fmt.Printf("error encoding links list to json: %s\n", err.Error())
			return
		}

		if err := ioutil.WriteFile("inbond_links.json", linkdata, 0667); err != nil {
			fmt.Printf("error writing file to json: %s\n", err.Error())
			return
		}

		fmt.Printf("found %d/%d inbound links for %s", found, checked, rawurl)
	},
}
