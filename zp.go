package zp

import (
	"bufio"
	"compress/gzip"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/miekg/dns"
)

// NSRecord represents a single record
type NSRecord struct {
	RType  string `db:"rtype"`  // record type: A, AAAA or NS by now
	Domain string `db:"domain"` // domain name without tld
	Value  string `db:"value"`  // value of the record: IPv4, IPv6 or nameserver domain name
	TLD    string `db:"tld"`    // com, name, ru etc
}

// DBRecord is the NSRecord with a date
type DBRecord struct {
	NSRecord
	Date time.Time `db:"date"` // actual data datetime
}

// NewRecord parses a line to a zone file record
func NewRecord(line string, tld string) (*NSRecord, error) {
	var (
		rtype string
		value string
		n     string
	)
	rr, err := dns.NewRR(line)
	if err != nil {
		return nil, err
	}

	if rr == nil {
		return nil, errors.New("empty record")
	}

	if n = rr.Header().Name; n == "" {
		return nil, errors.New("no domain found in the record")
	}

	switch rr := rr.(type) {
	case *dns.NS:
		rtype = "NS"
		value = rr.Ns
	case *dns.A:
		rtype = "A"
		value = rr.A.String()
	case *dns.AAAA:
		rtype = "AAAA"
		value = rr.AAAA.String()
	case *dns.TXT:
		rtype = "TXT"
		value = strings.Join(rr.Txt, ", ")
	default:
		return nil, errors.New("unsupported record type")
	}

	// if tld is an empty string we got zone from czds.icann.org or similar AXFR response
	if tld == "" {
		parts := dns.SplitDomainName(n)
		tld = parts[len(parts)-1]
		n = strings.Join(parts[0:len(parts)-1], ".")
	}

	return &NSRecord{
		Domain: n,
		RType:  rtype,
		Value:  value,
		TLD:    tld,
	}, nil
}

// FetchZoneFile fetches gzipped zone file and push NSRecord entries
// to a channel specified in the config
func FetchZoneFile(path string, tld string, rc chan NSRecord) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	g, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer g.Close()

	sc := bufio.NewScanner(g)
	for sc.Scan() {
		r, err := NewRecord(sc.Text(), tld)
		if err != nil {
			// log.Println(err)
			continue
		}
		rc <- *r
	}

	return nil
}
