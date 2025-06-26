package commands

import (
	"fmt"
	"strings"
	"time"

	chunkDb "github.com/Custocy/chunk-merger/internal/db"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

var ListCmd = &cobra.Command{
	Use:   "list",
	Short: "List chunks",
	Long:  "List chunks",
	Run: func(cmd *cobra.Command, args []string) {
		dbName := cmd.Flag("db").Value.String()
		tableName := cmd.Flag("table").Value.String()
		intervalEnd := cmd.Flag("interval-end").Value.String()

		List(dbName, tableName, intervalEnd)
	},
}

var MergeCmd = &cobra.Command{
	Use:   "merge",
	Short: "Merge chunks that belongs the same time period, day or month",
	Run: func(cmd *cobra.Command, args []string) {
		dbName := cmd.Flag("db").Value.String()
		tableName := cmd.Flag("table").Value.String()
		mergeType := cmd.Flag("type").Value.String()
		dryMode := cmd.Flag("dry-run").Value.String() == "true"
		intervalEnd := cmd.Flag("interval-end").Value.String()

		Merge(dbName, tableName, mergeType, intervalEnd, dryMode)
	},
}

func List(dbName, tableName, intervalEnd string) {
	db, err := chunkDb.Connect(dbName)
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}
	defer db.Close()

	chunks, err := chunkDb.GetChunks(db, dbName, tableName, intervalEnd)
	if err != nil {
		fmt.Println("Error getting chunks:", err)
		return
	}

	for _, chunk := range chunks {
		fmt.Printf("%s.%s %s -> %s\n", chunk.Schema, chunk.Name, chunk.StartRange.Format(time.RFC3339), chunk.EndRange.Format(time.RFC3339))
	}
}

func Merge(dbName, tableName, mergeType, intervalEnd string, dryMode bool) {
	db, err := chunkDb.Connect(dbName)
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}
	defer db.Close()

	chunks, err := chunkDb.GetChunks(db, dbName, tableName, intervalEnd)
	if err != nil {
		fmt.Println("Error getting chunks:", err)
		return
	}

	// for each chunk, find similar chunks
	done := make(map[string]bool)

	queriesNum := 0

	for _, chunk := range chunks {
		if done[chunk.Name] {
			continue
		}

		chunksGroup := make([]string, 0)

		for _, otherChunk := range chunks {
			isValid := false

			switch mergeType {
			case "exact":
				isValid = time.Time.Equal(otherChunk.StartRange, chunk.StartRange) && time.Time.Equal(otherChunk.EndRange, chunk.EndRange)
			case "day":
				y1, m1, d1 := chunk.StartRange.Date()
				y2, m2, d2 := otherChunk.StartRange.Date()

				isValid = y1 == y2 && m1 == m2 && d1 == d2
			case "month":
				y1, m1, _ := chunk.StartRange.Date()
				y2, m2, _ := otherChunk.StartRange.Date()

				isValid = y1 == y2 && m1 == m2
			}

			if otherChunk.Schema == chunk.Schema && isValid {
				chunksGroup = append(chunksGroup, fmt.Sprintf("%s.%s", otherChunk.Schema, otherChunk.Name))
				done[otherChunk.Name] = true
			}
		}

		if len(chunksGroup) <= 1 {
			continue
		}

		query := fmt.Sprintf("CALL merge_chunks('{%s}');", strings.Join(chunksGroup, ", "))

		if dryMode {
			fmt.Println("--", query)
			continue
		}

		_, err := chunkDb.ExecuteQuery(db, query)
		if err != nil {
			fmt.Println("Error executing query:", err)
			time.Sleep(time.Duration(1 * time.Second))
			continue
		}
		time.Sleep(time.Duration(10 * time.Second))

		queriesNum += 1
	}
}
