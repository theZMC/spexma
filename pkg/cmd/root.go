package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "spexma",
	Short: "Splunk Export Massager - Process and transform Splunk CSV exports",
	Long: `Splunk Export Massager (spexma) is a tool for processing Splunk CSV exports.
It provides various subcommands to transform, filter, and split your exports.`,
	Version: "1.0.0",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(splitCmd)
	rootCmd.AddCommand(publishCmd)
	rootCmd.AddCommand(hecTestCmd)
}
