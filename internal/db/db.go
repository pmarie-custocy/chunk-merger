package db

import (
	"database/sql"
	"fmt"
	"os"
	"time"
)

type Chunk struct {
	Schema     string
	Name       string
	StartRange time.Time
	EndRange   time.Time
}

func Connect(dbName string) (*sql.DB, error) {
	dsn := os.Getenv("DSN")
	if dsn == "" {
		dsn = "postgres://postgres@localhost:5432/" + dbName + "?sslmode=disable"
	}

	// connStr := fmt.Sprintf("user=postgres dbname=%s sslmode=disable", dbName)
	return sql.Open("postgres", dsn)
}

func ExecuteQuery(db *sql.DB, query string) (sql.Result, error) {
	fmt.Println("Executing query:", query)
	return db.Exec(query)
}

func GetChunks(dbInterface *sql.DB, dbName, tableName, intervalEnd string) ([]Chunk, error) {
	query := fmt.Sprintf(`
		SELECT chunk_schema, chunk_name, range_start, range_end
			FROM timescaledb_information.chunks
			WHERE hypertable_name = $1 AND range_end < now() - INTERVAL '%s'
			ORDER BY range_start`,
		intervalEnd,
	)
	rows, err := dbInterface.Query(query, tableName)

	if err != nil {
		fmt.Println("Error executing query:", err)
		return []Chunk{}, err
	}

	defer rows.Close()

	chunks := make([]Chunk, 0)

	for rows.Next() {
		var chunkSchema, chunkName string
		var rangeStart, rangeEnd time.Time

		err := rows.Scan(&chunkSchema, &chunkName, &rangeStart, &rangeEnd)
		if err != nil {
			fmt.Println("Error scanning row:", err)
			return []Chunk{}, err
		}

		// fmt.Printf("Chunk: %s.%s, Range: %v -> %v\n", chunkSchema, chunkName, rangeStart, rangeEnd)
		chunks = append(chunks, Chunk{
			Schema:     chunkSchema,
			Name:       chunkName,
			StartRange: rangeStart,
			EndRange:   rangeEnd,
		})
	}

	return chunks, nil
}
