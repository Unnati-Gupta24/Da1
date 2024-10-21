package da

import (
	"context"
	"database/sql"
	"encoding/hex"
	"log"
	"github.com/Layer-Edge/bitcoin-da/config"
	"github.com/ethereum/go-ethereum/ethclient"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/zeromq/goczmq.v4"
)

var db *sql.DB

// HashBlockSubscriber subscribes to hash blocks and writes transaction hashes to the database.
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

	StartListening(channeler, &HashBlockProcessor{layerEdgeClient: layerEdgeClient, config: cfg}, cfg.WriteIntervalBlock)
}

// HashBlockProcessor processes incoming hash block messages.
type HashBlockProcessor struct {
	layerEdgeClient *ethclient.Client
	config          *config.Config // Ensure this field exists.
}

// Process processes the hash block message.
func (proc *HashBlockProcessor) Process(msg [][]byte) error {
	hash, err := ProcessMsg(msg, proc.config.ProtocolId, proc.layerEdgeClient)
	if err != nil {
	    return err
	}

	statement := `insert into writer(txnHash) values($1);`
    _, err = db.Exec(statement, string(hash))
	return err
}

// ProcessMsg processes the incoming message and returns the transaction hash.
func ProcessMsg(msg [][]byte, protocolId string, layerEdgeClient *ethclient.Client) ([]byte, error) {
	layerEdgeHeader, err := layerEdgeClient.HeaderByNumber(context.Background(), nil)
	if err != nil {
	    log.Println("Error getting layerEdgeHeader: ", err)
	    return nil, err
	}
	dhash := layerEdgeHeader.Hash()
	log.Println("Latest LayerEdge Block Hash:", dhash.Hex())

	data := append([]byte(protocolId), dhash.Bytes()...)
	hash, err := CallScriptWithData(BashScriptPath, hex.EncodeToString(data), BtcCliPath)

	return hash, err
}
