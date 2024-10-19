package da

import (
	"log"
	"os"
	"os/exec"
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
	listen(processor RawBlockProcessor)
	validate() (bool, [][]byte)
}

// CallScriptWithData executes a bash script with the given data and returns the output.
func CallScriptWithData(scriptPath string, data string, btcCliPath string) ([]byte, error) {
	cmd := exec.Command(scriptPath+"/op_return_transaction.sh", data)
	cmd.Env = append(os.Environ(), "BTC_CLI_PATH="+btcCliPath)
	out, err := cmd.Output()
	return out, err
}

// ValidateMessage checks if the received message is valid.
func ValidateMessage(msg [][]byte) (bool, [][]byte) {
	if len(msg) != 3 {
		log.Println("Received message with unexpected number of parts")
		return false, nil
	}
	return true, msg
}
