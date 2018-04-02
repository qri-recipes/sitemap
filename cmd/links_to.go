package cmd

import (
	"fmt"

	// "github.com/qri-recipes/sitemap/sitemap"
	"github.com/spf13/cobra"
)

var LinksToCmd = &cobra.Command{
	Use:   "links-to",
	Short: "list links to a given url",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("not yet finished")
	},
}
