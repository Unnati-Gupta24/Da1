package da

import (
	"fmt"
    "bytes"
    "log"
	"github.com/btcsuite/btcd/wire"
	"gopkg.in/zeromq/goczmq.v4"
	"github.com/Layer-Edge/bitcoin-da/config"
    "github.com/Layer-Edge/bitcoin-da/utils"
)

func RawBlockSubscriber(cfg *config.Config) {
	channelReader := ZmqChannelReader{channeler: nil}
	processor := BitcoinBlockProcessor{}
	if channelReader.subscribe(cfg.ZmqEndpointRawBlock, "rawblock") == false {
		return
	}

	defer channelReader.clear()

	// Listen for messages
	channelReader.listen(processor, cfg.ProtocolId)
}

type ZmqChannelReader struct {
	channeler *goczmq.Channeler
}

func (zmqC *ZmqChannelReader) clear() {
	zmqC.channeler.Destroy()
}

func (zmqC *ZmqChannelReader) subscribe(endpoint string, typ string) bool {
	zmqC.channeler = goczmq.NewSubChanneler(endpoint, typ)
	if zmqC.channeler == nil {
		log.Fatal("Error creating channeler", zmqC.channeler)
		return false
	}
	fmt.Println("Subscribed to: ", endpoint, typ)
	return true
}

func (zmqC *ZmqChannelReader) validate() (bool, [][]byte) {
    msg, ok := <-zmqC.channeler.RecvChan
    // Validate
    if !ok {
        log.Println("Failed to receive message")
        return false, nil
    }
    if len(msg) != 3 {
        log.Println("Received message with unexpected number of parts")
        return false, nil
    }
    return true, msg
}

func (zmqC *ZmqChannelReader) listen(processor RawBlockProcessor, protocolId string) {
	fmt.Println("Listening for Raw Blocks (reader) from ZMQ channel...", zmqC.channeler)
	for {
		ok, msg := zmqC.validate()
		if !ok {
			continue
		}
		log.Println("Processing message")
		processor.process(msg, protocolId)
	}
}

type RawBlockProcessor interface {
	process(data [][]byte, protocolId string) bool
}

type BitcoinBlockProcessor struct{}

func (btcProc BitcoinBlockProcessor) process(msg [][]byte, protocolId string) bool {
	topic := string(msg[0])
	serializedBlock := msg[1]

	fmt.Printf("Topic: %s\n", topic)
	fmt.Printf("Serialized block: %x\n", serializedBlock)

	parsedBlock, err := parseBlock(serializedBlock)
	if err != nil {
		log.Printf("Failed to parse transaction: %v", err)
		return false
	}
	printBlock(parsedBlock)
	readPostedData(parsedBlock, []byte(protocolId))
	return true
}

// parseBlock parses a serialized Bitcoin block
func parseBlock(data []byte) (*wire.MsgBlock, error) {
    var block wire.MsgBlock
    err := block.Deserialize(bytes.NewReader(data))
    if err != nil {
        return nil, err
    }
    return &block, nil
}

func readPostedData(block *wire.MsgBlock, protocolId []byte) {
    var blobs [][]byte
    for _, tx := range block.Transactions {
		for _, txout := range tx.TxOut {
            pushData, err := utils.ExtractPushData(1, txout.PkScript)
            if err != nil {
                log.Println("failed to extract push data", err)
            }
            if pushData != nil && bytes.HasPrefix(pushData, protocolId) {
                blobs = append(blobs, pushData[:])
            }
        }
    }
    var data []string
    for _, blob := range blobs {
		data = append(data, fmt.Sprintf("%s:%x", blob[:len(protocolId)], blob[len(protocolId):]))
    }

    log.Println("Relayer Read: ", data)
}

// printBlock prints the details of a Bitcoin block
func printBlock(block *wire.MsgBlock) {
    fmt.Println("Block Details:")
    fmt.Printf("  Block Header:\n")
    fmt.Printf("    Version: %d\n", block.Header.Version)
    fmt.Printf("    Previous Block: %s\n", block.Header.PrevBlock)
    fmt.Printf("    Merkle Root: %s\n", block.Header.MerkleRoot)
    fmt.Printf("    Timestamp: %s\n", block.Header.Timestamp)
    fmt.Printf("    Bits: %d\n", block.Header.Bits)
    fmt.Printf("    Nonce: %d\n", block.Header.Nonce)

    fmt.Println("  Transactions:")
    for i, tx := range block.Transactions {
        fmt.Printf("    Transaction #%d:\n", i+1)
        printTransaction(tx)
    }
    fmt.Println("  Block Height: [unknown]")
}

// printTransaction prints the details of a Bitcoin transaction
func printTransaction(tx *wire.MsgTx) {
    fmt.Println("Transaction Details:")
    fmt.Printf("  Version: %d\n", tx.Version)
    fmt.Printf("  LockTime: %d\n", tx.LockTime)

    fmt.Println("  Inputs:")
    for i, txIn := range tx.TxIn {
        fmt.Printf("    Input #%d:\n", i+1)
        fmt.Printf("      Previous Outpoint: %s\n", txIn.PreviousOutPoint)
        fmt.Printf("      Signature Script: %x\n", txIn.SignatureScript)
        fmt.Printf("      Sequence: %d\n", txIn.Sequence)
    }

    fmt.Println("  Outputs:")
    for i, txOut := range tx.TxOut {
        fmt.Printf("    Output #%d:\n", i+1)
        fmt.Printf("      Value: %d\n", txOut.Value)
        fmt.Printf("      PkScript: %x\n", txOut.PkScript)
    }
}
