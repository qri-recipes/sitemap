package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Short: "CLI tool for building sitemaps",
	// Run: func(cmd *cobra.Command, args []string) {
	// },
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
}

func init() {
	RootCmd.AddCommand(
		CleanCmd,
		CrawlCmd,
		LinksToCmd,
		NormalizeUrlCmd,
		StatsCmd,
		UnfetchedUrlsCmd,
	)
}
