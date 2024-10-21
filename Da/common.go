package da

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"gopkg.in/zeromq/goczmq.v4"
)

// Global variables for script paths
var (
	BashScriptPath string
	BtcCliPath     string
)

// ChannelReader interface for ZeroMQ channel operations
type ChannelReader interface {
	subscribe(endpoint string, typ string) bool
	clear() bool
	validate() (bool, [][]byte)
}

// Define RawBlockProcessor interface here if not imported from another file.
type RawBlockProcessor interface {
	Process(msg [][]byte) error 
}

// CallScriptWithData executes a bash script with the given data and returns the output.
func CallScriptWithData(scriptPath string, data string, btcCliPath string) ([]byte, error) {
	cmd := exec.Command(scriptPath+"/op_return_transaction.sh", data)
	cmd.Env = append(os.Environ(), "BTC_CLI_PATH="+btcCliPath)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute script %s: %w", scriptPath, err)
	}
	return out, nil
}

// ValidateMessage checks if the received message is valid.
func ValidateMessage(msg [][]byte) (bool, [][]byte) {
	if len(msg) != 3 {
		log.Println("Received message with unexpected number of parts")
		return false, nil
	}
	return true, msg
}

// StartListening starts listening for messages on the given channeler and processes them.
func StartListening(channeler *goczmq.Channeler, processor RawBlockProcessor, writeInterval int) {
	counter := 0

	for {
		msg, ok := <-channeler.RecvChan
		if !ok {
			log.Println("Failed to receive message")
			continue
		}
		if (counter % writeInterval) != 0 {
			continue
		}

		valid, msg := ValidateMessage(msg)
		if !valid {
			continue
		}

		log.Println("Processing message")
		err := processor.Process(msg)
		if err != nil {
			log.Println("Error processing message:", err)
			continue
		}

		counter++
	}
}
