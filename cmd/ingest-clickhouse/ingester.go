package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ilyaglow/zp"
	_ "github.com/mailru/go-clickhouse"
)

const (
	insertStatement = `INSERT INTO dnszones (rtype, domain, value, tld) VALUES (?, ?, ?, ?)`
	createStatement = `
		CREATE TABLE IF NOT EXISTS dnszones (
			rtype	String,
			domain	String,
			value	String,
			tld		String,
			date	Date DEFAULT today()
		) engine=MergeTree(date,(domain,tld),8192)
	`
	zoneExtension         = "gz"
	exceptionZoneFileName = "com.zone.gz"
	exceptionZone         = "com"
	tSize                 = 10000
)

func main() {
	sd := flag.String("f", ".", "Directory with zone files with .gz extension")
	ch := flag.String("c", "http://127.0.0.1:8123/default", "Clickhouse URL")
	nw := flag.Int("workers", 4, "Number of sending workers")
	flag.Parse()

	conn, err := sql.Open("clickhouse", *ch)
	if err != nil {
		log.Fatal(err)
	}

	if err := conn.Ping(); err != nil {
		log.Fatal(err)
	}
	_, err = conn.Exec(createStatement)

	rc := make(chan zp.NSRecord)

	var wg sync.WaitGroup
	wg.Add(*nw)

	for i := 1; i <= *nw; i++ {
		go func() {
			defer wg.Done()
			send(conn, rc)
		}()
	}

	filepath.Walk(*sd, func(path string, fi os.FileInfo, err error) error {
		if !strings.HasSuffix(path, zoneExtension) {
			return nil
		}

		if strings.HasSuffix(path, exceptionZoneFileName) {
			if err := zp.FetchZoneFile(path, exceptionZone, rc); err != nil {
				log.Fatal(err)
			}
		} else {
			if err := zp.FetchZoneFile(path, "", rc); err != nil {
				log.Fatal(err)
			}
		}

		return nil
	})

	close(rc)
	wg.Wait()
}

func send(conn *sql.DB, input <-chan zp.NSRecord) {
	var it uint

	tx, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(insertStatement)
	if err != nil {
		log.Fatal(err)
	}

	for rec := range input {
		if _, err := stmt.Exec(
			rec.RType,
			rec.Domain,
			rec.Value,
			rec.TLD); err != nil {
			log.Fatal(err)
		}

		it++

		if it == tSize {
			log.Printf("Commit transaction with %d entries", tSize)
			it = 0
			if err := tx.Commit(); err != nil {
				if strings.Contains(err.Error(), "Transaction") {
					log.Println(err)
				} else {
					log.Println("tx.Commit() failed")
					log.Fatal(err)
				}
			}
			tx, err = conn.Begin()
			if err != nil {
				log.Fatal(err)
			}
			stmt, err = tx.Prepare(insertStatement)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	log.Println("Committing the tail")
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

}
