package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
)

type triggerDef struct {
	fnName  string
	tgName  string
	table   string
	channel string
	filter  string // optional WHERE condition on NEW
}

var triggers = []triggerDef{
	{
		fnName:  "notify_queue_insert",
		tgName:  "queue_insert_trigger",
		table:   "queue",
		channel: "queue_insert",
	},
	{
		fnName:  "notify_done_insert",
		tgName:  "done_insert_trigger",
		table:   "done",
		channel: "done_insert",
		filter:  "NEW.item::text LIKE '%packet_recv%'",
	},
	{
		fnName:  "notify_failed_insert",
		tgName:  "failed_insert_trigger",
		table:   "failed",
		channel: "failed_insert",
	},
}


func checkTgSQL(table, name string) string {
	return `SELECT EXISTS (
		SELECT 1 FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		WHERE c.relname = '` + table + `' AND t.tgname = '` + name + `'
	)`
}

func createFnSQL(fnName, channel, filter string) string {
	body := `PERFORM pg_notify('` + channel + `', NEW.id::text);`
	if filter != "" {
		body = `IF ` + filter + ` THEN
		` + body + `
	END IF;`
	}
	return `CREATE OR REPLACE FUNCTION ` + fnName + `()
	RETURNS trigger AS $$
	BEGIN
		` + body + `
		RETURN NEW;
	END;
	$$ LANGUAGE plpgsql;`
}

func createTgSQL(tgName, table, fnName string) string {
	return `CREATE TRIGGER ` + tgName + `
	AFTER INSERT ON ` + table + `
	FOR EACH ROW EXECUTE FUNCTION ` + fnName + `();`
}

func main() {
	cfgPath := flag.String("config", "config.toml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	relayerDB, err := db.NewPool(ctx, cfg.RelayerDB)
	if err != nil {
		log.Fatalf("relayer db: %v", err)
	}
	defer relayerDB.Close()

	for _, t := range triggers {
		// Always re-run CREATE OR REPLACE FUNCTION so filter changes take effect.
		if _, err := relayerDB.Exec(ctx, createFnSQL(t.fnName, t.channel, t.filter)); err != nil {
			log.Fatalf("create function %s: %v", t.fnName, err)
		}
		log.Printf("function ready: %s", t.fnName)

		// Trigger is not idempotent — only create if missing.
		// Existing trigger automatically picks up the updated function.
		var tgExists bool
		if err := relayerDB.QueryRow(ctx, checkTgSQL(t.table, t.tgName)).Scan(&tgExists); err != nil {
			log.Fatalf("check trigger %s: %v", t.tgName, err)
		}
		if tgExists {
			log.Printf("trigger already exists: %s", t.tgName)
			continue
		}
		if _, err := relayerDB.Exec(ctx, createTgSQL(t.tgName, t.table, t.fnName)); err != nil {
			log.Fatalf("create trigger %s: %v", t.tgName, err)
		}
		log.Printf("created trigger: %s on %s", t.tgName, t.table)
	}

	log.Println("setup complete")
}
