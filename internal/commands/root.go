package commands

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	RootCmd.AddCommand(ListCmd)
	RootCmd.AddCommand(MergeCmd)

	RootCmd.PersistentFlags().String("db", "custocy", "Database name")
	RootCmd.PersistentFlags().String("table", "object_event", "Table name")
	RootCmd.PersistentFlags().String("interval-end", "1 month", "Interval end for chunk selection")

	MergeCmd.PersistentFlags().Bool("dry-run", false, "Dry run mode, do not execute any changes")
	MergeCmd.Flags().String("type", "day", "Type of merge: exact, day, month")
}

var RootCmd = &cobra.Command{
	Use:   "chunk-merger",
	Short: "chunk-merger is a tool to merge chunks of timescale tables",
	Run: func(cmd *cobra.Command, args []string) {
		// Show help and leave
		cmd.Help()
	},
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
