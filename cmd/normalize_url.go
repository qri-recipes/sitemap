package cmd

import (
	"fmt"
	"net/url"

	"github.com/qri-recipes/sitemap/sitemap"
	"github.com/spf13/cobra"
)

var NormalizeUrlCmd = &cobra.Command{
	Use:   "normalize-url",
	Short: "transform one or more urls into it's normalized form",
	Run: func(cmd *cobra.Command, args []string) {
		var (
			u   *url.URL
			err error
		)

		for _, rawurl := range args {
			u, err = url.Parse(rawurl)
			if err != nil {
				fmt.Errorf("error parsing url:\n\t%s\n\t%s", rawurl, err.Error())
				return
			}
			fmt.Println(sitemap.NormalizeURLString(u))
		}
	},
}
