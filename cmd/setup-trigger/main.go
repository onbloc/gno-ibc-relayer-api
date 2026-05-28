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

const (
	checkFunctionSQL = `
		SELECT EXISTS (
			SELECT 1 FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = 'public' AND p.proname = 'notify_queue_insert'
		)
	`

	checkTriggerSQL = `
		SELECT EXISTS (
			SELECT 1 FROM pg_trigger t
			JOIN pg_class c ON c.oid = t.tgrelid
			WHERE c.relname = 'queue' AND t.tgname = 'queue_insert_trigger'
		)
	`

	createFunctionSQL = `
		CREATE OR REPLACE FUNCTION notify_queue_insert()
		RETURNS trigger AS $$
		BEGIN
			PERFORM pg_notify('queue_insert', NEW.id::text);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`

	createTriggerSQL = `
		CREATE TRIGGER queue_insert_trigger
		AFTER INSERT ON queue
		FOR EACH ROW EXECUTE FUNCTION notify_queue_insert();
	`
)

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

	// check function
	var fnExists bool
	if err := relayerDB.QueryRow(ctx, checkFunctionSQL).Scan(&fnExists); err != nil {
		log.Fatalf("check function: %v", err)
	}

	// check trigger
	var tgExists bool
	if err := relayerDB.QueryRow(ctx, checkTriggerSQL).Scan(&tgExists); err != nil {
		log.Fatalf("check trigger: %v", err)
	}

	if fnExists && tgExists {
		log.Println("trigger already set up: notify_queue_insert function and queue_insert_trigger are both present")
		return
	}

	if !fnExists {
		if _, err := relayerDB.Exec(ctx, createFunctionSQL); err != nil {
			log.Fatalf("create function: %v", err)
		}
		log.Println("created function: notify_queue_insert")
	} else {
		log.Println("function already exists: notify_queue_insert")
	}

	if !tgExists {
		if _, err := relayerDB.Exec(ctx, createTriggerSQL); err != nil {
			log.Fatalf("create trigger: %v", err)
		}
		log.Println("created trigger: queue_insert_trigger on queue table")
	} else {
		log.Println("trigger already exists: queue_insert_trigger")
	}

	log.Println("setup complete: voyager queue will now notify on insert")
}
