package da

import (
	"fmt"
    "log"
    "github.com/ethereum/go-ethereum/ethclient"
    "gopkg.in/zeromq/goczmq.v4"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    "github.com/Layer-Edge/bitcoin-da/config"
	"context"
	"encoding/hex"
)

var db *sql.DB

func HashBlockSubscriber(cfg *config.Config) {
    var err error
    db, err = sql.Open("sqlite3", "da.db")
    if err != nil {
        panic(err)
    }

    statement := `CREATE TABLE IF NOT EXISTS writer (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        txnHash TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
    );`
    
    _, err = db.Exec(statement)
    if err != nil {
        log.Fatalf("Error creating table: %v", err)
    }

    channeler := goczmq.NewSubChanneler(cfg.ZmqEndpointHashBlock, "hashblock")

    if channeler == nil {
        log.Fatal("Error creating channeler", channeler)
    }
    defer channeler.Destroy()

    layerEdgeClient, err := ethclient.Dial(cfg.LayerEdgeRPC.HTTP)
    if err != nil {
        log.Fatal("Error creating layerEdgeClient: ", err)
    }

    counter := 0

    for {
        select {
        case msg, ok := <-channeler.RecvChan:
            if !ok {
                log.Println("Failed to receive message")
                continue
            }
            if (counter % cfg.WriteIntervalBlock) != 0 {
                continue
            }
            if len(msg) != 3 {
                log.Println("Received message with unexpected number of parts")
                continue
            }
            hash, err := ProcessMsg(msg, cfg.ProtocolId, layerEdgeClient)
            if err != nil {
                log.Println("Error writing -> ", err)
                continue
            }
            counter++
            log.Println("Relayer Write Done -> ", hash)

            statement := `insert into writer(txnHash) values($1);`
            _, err = db.Exec(statement, string(hash))
        }
    }
}

func ProcessMsg(msg [][]byte, protocolId string, layerEdgeClient *ethclient.Client) ([]byte, error) {
	// Split the message into topic, serialized transaction, and sequence number
	topic := string(msg[0])

	// Print out the parts
	fmt.Printf("Topic: %s\n", topic)

	layerEdgeHeader, err := layerEdgeClient.HeaderByNumber(context.Background(), nil)
	if err != nil {
		log.Println("Error getting layerEdgeHeader: ", err)
		return nil, err
	}
	dhash := layerEdgeHeader.Hash()
	log.Println("Latest LayerEdge Block Hash:", dhash.Hex())

	data := append([]byte(protocolId), dhash.Bytes()...)
	fmt.Println(data)

	// CallScriptWithData requires three arguments: scriptPath, data, and btcCliPath
	hash, err := CallScriptWithData(BashScriptPath, hex.EncodeToString(data), BtcCliPath) // Updated line

	return hash, err
}
