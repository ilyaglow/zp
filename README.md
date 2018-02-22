zp - zone files parser
----------------------

Handy helper to parse zone files into some data store.

An [example](cmd/ingest-clickhouse/ingester.go) shows how to send records to a fast [Clickhouse](https://clickhouse.yandex) SQL-server.

# Usage

No need to say that you should get zone files first (czds.icann.org, verisign etc).

## Start Clickhouse

Start the server:
```
docker run -itd \
           --name some-clickhouse-server \
           --ulimit nofile=262144:262144 \
           -v your-persistent-storage:/var/lib/clickhouse/data \
           -p 127.0.0.1:8123:8123 \
           yandex/clickhouse-server
```

Get a Clickhouse SQL shell:
```
docker run -it \
           --rm \
           --link some-clickhouse-server:clickhouse-server \
           yandex/clickhouse-client --host clickhouse-server
```

## Ingest zonefiles

You do not need to unpack `.gz` files, the example will do it for you.

```
go run cmd/ingest-clickhouse/ingester.go -h
  -c string
    	Clickhouse URL (default "http://127.0.0.1:8123/default")
  -f string
    	Directory with zone files with .gz extension (default ".")
  -workers int
    	Number of sending workers (default 4)
```
