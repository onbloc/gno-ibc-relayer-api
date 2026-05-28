package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
)

const targetAddress = "g1jg8mtutu9khhfwc4nxmuhcpftf0pajdhfvsqf5"

var (
	gnoAddresses = []string{
		"g1us8428u2a5satrlxzagqqa5m6vmuze025anjlr",
		"g1u7y667z64x2h7vc6fmpcprgey4ck233jaww9zq",
		"g14fclnpgzexnap4xqk4nwcxh5we74y6eypjwds5",
		"g1manfred42lqcl8rke8z601scx53q8q87jkn4ph",
		"g1f4v282mwyhu29afke4vq5r2xzcm6z3ftnugcnv",
		"g15gdm49ktawvkrl88jadqpucng37yxutucuwaef",
		"g1k2lg9r9kwfzfgggfxr96sk5kkqmqdnqkr8ufr7",
		"g1n4yvwnv77frq2ccuw27dmtjkd7u8p6htcpvnxj",
	}

	ethAddresses = []string{
		"0xf4ad3b02d44fa88371ef8faa232f789068b5f56b",
		"0x7fed1d819109fb7a095137bf867abe61db36c99c",
		"0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc",
		"0x90f79bf6eb2c4f870365e785982e1f101e93b906",
		"0x15d34aaf54267db7d7c367839aaf71a00a2c6a65",
		"0x9965507d1a55bcc2695c58ba16fb37d819b0a4dc",
		"0x976ea74026e726554db657fa54763abd0c3a0aa9",
		"0x14dc79964da2c08b23698b3d3cc7ca32193d9955",
	}

	gnoToken = "ugnot"
	ethToken = "0x7f5c764cbc14f9669b88837ca1490cca17c31607"
)

func randHex(n int) string {
	const chars = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func randAmount() string {
	amounts := []string{"1000000", "5000000", "10000000", "50000000", "100000000", "500000000"}
	return amounts[rand.Intn(len(amounts))]
}

func main() {
	cfgPath := flag.String("config", "config.toml", "path to config file")
	truncate := flag.Bool("truncate", false, "truncate transfers table before seeding")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	appDB, err := db.NewPool(ctx, cfg.AppDB)
	if err != nil {
		log.Fatalf("app db: %v", err)
	}
	defer appDB.Close()

	if *truncate {
		if _, err := appDB.Exec(ctx, `TRUNCATE transfers RESTART IDENTITY`); err != nil {
			log.Fatalf("truncate: %v", err)
		}
		log.Println("seed: transfers table truncated")
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	now := time.Now().UTC()

	inserted := 0
	for i := 0; i < 100; i++ {
		id := int64(80000000 + i*1000 + rng.Intn(999))
		packetHash := "0x" + randHex(64)
		txHash := "0x" + randHex(64)
		height := int64(80000 + rng.Intn(5000))
		timeout := now.Add(72 * time.Hour).UnixNano()
		createdAt := now.Add(-time.Duration(rng.Intn(72)) * time.Hour)

		// 상태 분포: done 50%, processing 20%, detected 20%, failed 10%
		statusRoll := rng.Intn(10)
		var status int
		switch {
		case statusRoll < 5:
			status = 2 // done
		case statusRoll < 7:
			status = 1 // processing
		case statusRoll < 9:
			status = 0 // detected
		default:
			status = 3 // failed
		}

		var doneAt *time.Time
		if status == 2 {
			t := createdAt.Add(time.Duration(rng.Intn(300)+10) * time.Second)
			doneAt = &t
		}

		var errMsg *string
		if status == 3 {
			msgs := []string{
				"error in voyager-client-update-plugin-state-lens/state-lens/ics23/ics23: error in state/ibc-union/union-testnet-10: client `39` not found",
				"error in voyager-client-update-plugin-state-lens/state-lens/ics23/mpt: error in state/ibc-union/union-testnet-10: client `4` not found",
				"timeout waiting for packet receipt on destination chain",
			}
			m := msgs[rng.Intn(len(msgs))]
			errMsg = &m
		}

		// 방향: gno→eth / eth→gno 반반
		// targetAddress를 i < 20 이면 from 또는 to에 강제 배치
		var srcChain, dstChain, fromAddr, toAddr, baseToken, quoteToken string
		var srcChannel, dstChannel int

		gnoToEth := rng.Intn(2) == 0
		if gnoToEth {
			srcChain, dstChain = "dev", "11155111"
			srcChannel, dstChannel = 2, 28
			baseToken, quoteToken = gnoToken, ethToken
			if i < 20 {
				fromAddr = targetAddress
				toAddr = ethAddresses[rng.Intn(len(ethAddresses))]
			} else {
				fromAddr = gnoAddresses[rng.Intn(len(gnoAddresses))]
				toAddr = ethAddresses[rng.Intn(len(ethAddresses))]
			}
		} else {
			srcChain, dstChain = "11155111", "dev"
			srcChannel, dstChannel = 28, 2
			baseToken, quoteToken = ethToken, gnoToken
			if i < 20 {
				fromAddr = ethAddresses[rng.Intn(len(ethAddresses))]
				toAddr = targetAddress
			} else {
				fromAddr = ethAddresses[rng.Intn(len(ethAddresses))]
				toAddr = gnoAddresses[rng.Intn(len(gnoAddresses))]
			}
		}

		amount := randAmount()

		_, err := appDB.Exec(ctx, `
			INSERT INTO transfers (
				id, packet_hash,
				src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
				from_address, to_address,
				base_token, base_amount, quote_token, quote_amount,
				height, tx_hash, timeout_timestamp,
				status, created_at, done_at, err_msg
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19
			) ON CONFLICT (id) DO NOTHING`,
			id, packetHash,
			srcChain, dstChain, srcChannel, dstChannel,
			fromAddr, toAddr,
			baseToken, amount, quoteToken, amount,
			height, txHash, timeout,
			status, createdAt, doneAt, errMsg,
		)
		if err != nil {
			log.Printf("seed: insert i=%d: %v", i, err)
			continue
		}
		inserted++
	}

	fmt.Printf("seed: inserted %d/100 transfers\n", inserted)
	fmt.Printf("seed: %s appears in at least 20 rows\n", targetAddress)
}
