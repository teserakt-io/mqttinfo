package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/pflag"

	au "github.com/logrusorgru/aurora"
	mqttinfo "gitlab.com/teserakt/mqttinfo/pkg/mqttinfolib"
)

// variables set at build time
var gitCommit string
var gitTag string
var buildDate string

const (
	v4 = "MQTT v3.1.1"
	v5 = "MQTT v5.0"
)

func res(val bool) interface{} {
	if val {
		return au.Green("YES")
	} else {
		return au.Red("NO")
	}
}

func main() {

	fs := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)

	hostname := fs.StringP("host", "h", "localhost", "MQTT broker to connect to")
	port := fs.IntP("port", "p", 1883, "network port to connect to")
	username := fs.StringP("user", "u", "", "username, if authentication is needed")
	password := fs.StringP("pwd", "P", "", "password, if authentication is needed")
	help := fs.BoolP("help", "", false, "shows this")
	jsonout := fs.BoolP("json", "j", false, "writes JSON-formatted output to mqttinfo.json")

	fs.Parse(os.Args[1:])

	if *help {
		fs.PrintDefaults()
		return
	}

	if len(*username) >= 0x10000 ||
		len(*password) >= 0x10000 ||
		*port >= 0x10000 {
		fs.PrintDefaults()
		return
	}

	if len(gitTag) == 0 {
		fmt.Printf("MQTTinfo – version %v-%v\n", buildDate, gitCommit[:4])
	} else {
		fmt.Printf("MQTTinfo – version %s (%v-%v)\n", gitTag, buildDate, gitCommit[:4])
	}

	fmt.Println("Copyright (c) Teserakt AG, 2019")

	// info will hold the results of the analysis
	b := mqttinfo.NewBrokerInfo(*hostname, *port, *username, *password)

	fmt.Printf("\nTarget: %v:%v\n", b.Host, b.Port)

	var err error

	// v3.1.1 tests
	fmt.Printf("\nChecking %v broker interface...\n", v4)
	err = b.CheckConnectionV4()
	if err != nil {
		fmt.Printf("%v check failed: %v\n", v4, err)
		b.Failed = true
		b.Error = err.Error()
		return
	} else {
		fmt.Printf("%v support\t%v\n", v4, res(b.V4))
		if b.V4 {
			fmt.Printf("needs authentication\t%v\n", res(!b.V4Anonymous))
		}
	}

	// v5.0 tests
	fmt.Printf("\nChecking %v broker interface...\n", v5)
	err = b.CheckConnectionV5()
	if err != nil {
		fmt.Printf("%v check failed: %v\n", v5, err)
		b.Failed = true
		b.Error = err.Error()
		return
	} else {
		fmt.Printf("%v support\t%v\n", v5, res(b.V5))
		if b.V5 {
			fmt.Printf("needs authentication\t%v\n", res(!b.V5Anonymous))
		}
	}

	if b.V4 {
		fmt.Printf("\nAnalyzing %v broker interface...\n", v4)
		err = b.AnalyzeV4()
		if err != nil {
			fmt.Printf("Analysis failed: %v\n", err)
			b.Failed = true
			b.Error = err.Error()
			return
		} else {

			// Shows correct behavior as OK/green
			fmt.Printf("supports QoS1\t\t%v\n", res(b.V4QoS1))
			fmt.Printf("supports QoS2\t\t%v\n", res(b.V4QoS2))
			fmt.Printf("rejects QoS3\t\t%v\n", res(!b.V4QoS3Response))
			fmt.Printf("forbids subscribe to #\t%v\n", res(!b.V4SubscribeAll))
			fmt.Printf("rejects invalid topic\t%v\n", res(!b.V4InvalidTopics))
			fmt.Printf("rejects invalid UTF-8\t%v\n", res(!b.V4InvalidUTF8Topic))
			fmt.Printf("rejects $SYS publishs\t%v\n", res(!b.V4PublishSYS))
			if b.V4PublishSYS {
				fmt.Printf("filters $SYS publishs\t%v\n", res(b.V4FilterSYS))
			}
		}
	}

	if b.V5 {
		fmt.Printf("\nAnalyzing %v broker interface...\n", v5)
		err = b.AnalyzeV5()
		if err != nil {
			fmt.Printf("Analysis failed: %v\n", err)
			b.Failed = true
			b.Error = err.Error()
			return
		} else {

			// Shows correct behavior as OK/green
			fmt.Printf("supports QoS1\t\t%v\n", res(b.V5QoS1))
			fmt.Printf("supports QoS2\t\t%v\n", res(b.V5QoS2))
			fmt.Printf("rejects QoS3\t\t%v\n", res(!b.V5QoS3Response))
			fmt.Printf("forbids subscribe to #\t%v\n", res(!b.V5SubscribeAll))
			fmt.Printf("rejects invalid topic\t%v\n", res(!b.V5InvalidTopics))
			fmt.Printf("rejects invalid UTF-8\t%v\n", res(!b.V5InvalidUTF8Topic))
			fmt.Printf("rejects $SYS publishs\t%v\n", res(!b.V5PublishSYS))
			if b.V5PublishSYS {
				fmt.Printf("filters $SYS publishs\t%v\n", res(b.V5FilterSYS))
			}
		}
	}

	fmt.Println("\nTrying to guess broker software...")
	err = b.GuessBroker()
	if err != nil {
		fmt.Printf("Failed: %v\n", err)
	} else {
		fmt.Printf("looks like %v\n", b.TypeGuessed)
	}

	if !*jsonout {
		return
	}

	js, err := json.Marshal(b)
	if err != nil {
		fmt.Println(err)
	}
	file, err := os.OpenFile("mqttinfo.json", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error opening mqttinfo.json: %v\n", err)
		return
	}
	defer file.Close()
	_, err = file.WriteString(string(js) + "\n")
	if err != nil {
		fmt.Printf("Error writing JSON: %v\n", err)
		return
	}

}
