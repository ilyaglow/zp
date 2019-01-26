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
	_ "github.com/kshvakov/clickhouse"
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
		) engine=MergeTree(date,(value,rtype),8192)
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
	if _, err = conn.Exec(createStatement); err != nil {
		log.Fatal(err)
	}

	rc := make(chan zp.Record)

	var wg sync.WaitGroup
	wg.Add(*nw)

	for i := 0; i < *nw; i++ {
		go func() {
			defer wg.Done()
			if err := send(conn, rc); err != nil {
				log.Println(err)
			}
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

func send(conn *sql.DB, input <-chan zp.Record) error {
	var it uint

	tx, err := conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(insertStatement)
	if err != nil {
		return err
	}

	for rec := range input {
		if _, err := stmt.Exec(
			rec.RType,
			rec.Domain,
			rec.Value,
			rec.TLD); err != nil {
			return err
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
					return err
				}
			}
			tx, err = conn.Begin()
			if err != nil {
				return err
			}
			stmt, err = tx.Prepare(insertStatement)
			if err != nil {
				return err
			}
		}
	}

	log.Println("Committing the tail")
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
