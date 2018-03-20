package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	// GitCommit is the git reference injected at build
	GitCommit string
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the git ref",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(GitCommit)
	},
}
