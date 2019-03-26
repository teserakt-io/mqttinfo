package mqttinfo

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"
)

// Broker ...
type Broker string

// BrokerInfo includes the information collected
type BrokerInfo struct {
	Host     string
	Port     int
	Username string
	Password string

	V4 bool
	V5 bool

	V4Anonymous        bool
	V4PublishSYS       bool
	V4FilterSYS        bool
	V4SubscribeAll     bool
	V4InvalidTopics    bool
	V4InvalidUTF8Topic bool
	V4QoS1             bool
	V4QoS2             bool
	V4QoS3Response     bool

	V5Anonymous        bool
	V5PublishSYS       bool
	V5FilterSYS        bool
	V5SubscribeAll     bool
	V5InvalidTopics    bool
	V5InvalidUTF8Topic bool
	V5QoS1             bool
	V5QoS2             bool
	V5QoS3Response     bool

	TypeGuessed Broker

	// So that JSON line reports errors
	Failed bool
	Error  string
}

const (
	connectV4          = "\x10\x14\x00\x04\x4d\x51\x54\x54\x04\x02\x00\x3c\x00\x08\x6d\x71\x74\x74\x69\x6e\x66\x6f"
	connectV5          = "\x10\x15\x00\x04\x4d\x51\x54\x54\x05\x02\x00\x3c\x00\x00\x08\x6d\x71\x74\x74\x69\x6e\x66\x6f"
	pingreq            = "\xc0\x00"
	pingresp           = "\xd0\x00"
	publishV4Q0        = "\x32\x04\x00\x01\x41\x42"
	publishV4Q1        = "\x32\x06\x00\x01\x41\x00\x01\x42"
	publishV4Q2        = "\x34\x06\x00\x01\x41\x00\x01\x42"
	publishV4Q3        = "\x36\x06\x00\x01\x41\x00\x01\x42"
	publishV5Q1        = "\x32\x07\x00\x01\x41\x00\x01\x00\x42"
	publishV5Q2        = "\x34\x07\x00\x01\x41\x00\x01\x00\x42"
	publishV5Q3        = "\x36\x07\x00\x01\x41\x00\x01\x00\x42"
	pubackV4Q1         = "\x40\x02\x00\x01"
	pubackV5Q1a        = "\x40\x02\x00\x01"
	pubackV5Q1b        = "\x40\x03\x00\x01\x10"
	pubrecV4Q2         = "\x50\x02\x00\x01"
	pubrecV5Q2a        = "\x50\x02\x00\x01"
	pubrecV5Q2b        = "\x50\x03\x00\x01\x10"
	pubrelV4Q2         = "\x62\x02\x00\x01"
	pubrelV5Q2         = "\x62\x03\x00\x01\x00"
	pubcompV4Q2        = "\x70\x02\x00\x01"
	pubcompV5Q2        = "\x70\x02\x00\x01"
	subAllV4Q0         = "\x82\x06\x00\x01\x00\x01\x23\x00"
	subAllV5Q0         = "\x82\x07\x00\x01\x00\x00\x01\x23\x00"
	subackV4Q0         = "\x90\x03\x00\x01\x00"
	subackV5Q0         = "\x90\x04\x00\x01\x00\x00"
	subackV4Q1         = "\x90\x03\x00\x01\x01"
	subackV5Q1         = "\x90\x04\x00\x01\x00\x01"
	subInvalidV4Q0     = "\x82\x07\x00\x01\x00\x02\x41\x2b\x00"
	subInvalidV5Q0     = "\x82\x08\x00\x01\x00\x00\x02\x41\x2b\x00"
	subInvalidUTF8V4Q0 = "\x82\x07\x00\x01\x00\x02\xc3\x28\x00"
	subInvalidUTF8V5Q0 = "\x82\x08\x00\x01\x00\x00\x02\xc3\x28\x00"
	pubSysV4Q1         = "\x33\x12\x00\x0d\x24\x53\x59\x53\x2f\x6d\x71\x74\x74\x69\x6e\x66\x6f\x00\x01\x41"
	pubSysV5Q1         = "\x33\x13\x00\x0d\x24\x53\x59\x53\x2f\x6d\x71\x74\x74\x69\x6e\x66\x6f\x00\x01\x00\x41"
	subSysAV4Q0        = "\x82\x12\x00\x01\x00\x0d\x24\x53\x59\x53\x2f\x6d\x71\x74\x74\x69\x6e\x66\x6f\x01"
	subSysAV5Q0        = "\x82\x13\x00\x01\x00\x00\x0d\x24\x53\x59\x53\x2f\x6d\x71\x74\x74\x69\x6e\x66\x6f\x01"
	subSysAllV4Q0      = "\x82\x0b\x00\x01\x00\x06\x24\x53\x59\x53\x2f\x23\x00"
	subSysAllV5Q0      = "\x82\x0c\x00\x01\x00\x00\x06\x24\x53\x59\x53\x2f\x23\x00"
	subSysVerneV4Q0    = "\x82\x20\x00\x01\x00\x1b\x24\x53\x59\x53\x2f\x2b\x2f\x72\x6f\x75\x74\x65\x72\x2f\x73\x75\x62\x73\x63\x72\x69\x70\x74\x69\x6f\x6e\x73\x00"
	subSysVerneV5Q0    = "\x82\x21\x00\x01\x00\x00\x1b\x24\x53\x59\x53\x2f\x2b\x2f\x72\x6f\x75\x74\x65\x72\x2f\x73\x75\x62\x73\x63\x72\x69\x70\x74\x69\x6f\x6e\x73\x00"
	subSysMosqV4Q0     = "\x82\x20\x00\x01\x00\x1b\x24\x53\x59\x53\x2f\x2b\x2f\x6c\x6f\x61\x64\x2f\x6d\x65\x73\x73\x61\x67\x65\x73\x2f\x73\x65\x6e\x74\x2f\x2b\x00"
	subSysMosqV5Q0     = "\x82\x21\x00\x01\x00\x00\x1b\x24\x53\x59\x53\x2f\x2b\x2f\x6c\x6f\x61\x64\x2f\x6d\x65\x73\x73\x61\x67\x65\x73\x2f\x73\x65\x6e\x74\x2f\x2b\x00"
)

// NewBrokerInfo creates a BrokerInfo with default values
func NewBrokerInfo(hostname string, port int, username, password string) BrokerInfo {
	b := BrokerInfo{}
	b.Host = hostname
	b.Port = port
	b.Username = username
	b.Password = password

	b.V4 = false
	b.V5 = false

	b.V4Anonymous = false
	b.V4PublishSYS = false
	b.V4FilterSYS = true
	b.V4SubscribeAll = false
	b.V4InvalidTopics = false
	b.V4InvalidUTF8Topic = false
	b.V4QoS1 = false
	b.V4QoS2 = false
	b.V4QoS3Response = false

	b.V5Anonymous = false
	b.V5PublishSYS = false
	b.V5FilterSYS = true
	b.V5SubscribeAll = false
	b.V5InvalidTopics = false
	b.V5InvalidUTF8Topic = false
	b.V5QoS1 = false
	b.V5QoS2 = false
	b.V5QoS3Response = false

	b.TypeGuessed = "unknown"

	b.Failed = false
	b.Error = ""

	return b
}

// varint encoding, this function copied from https://github.com/eclipse/paho.mqtt.golang
func encodeLength(length int) []byte {
	var encLength []byte
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		encLength = append(encLength, digit)
		if length == 0 {
			break
		}
	}
	return encLength
}

func (b *BrokerInfo) getConnectV4() []byte {

	if b.Username == "" {
		return []byte(connectV4)
	}

	// Length were checked in main()
	usernameLen := uint16(len(b.Username))
	pwdLen := uint16(len(b.Password))

	remainingLen := 24 + int(usernameLen+pwdLen)

	remainingLenBytes := encodeLength(remainingLen)

	packet := make([]byte, 1)
	packet[0] = 0x10

	usernameLenBytes := make([]byte, 2)
	pwdLenBytes := make([]byte, 2)

	binary.BigEndian.PutUint16(usernameLenBytes, usernameLen)
	binary.BigEndian.PutUint16(pwdLenBytes, pwdLen)

	packet = append(packet, remainingLenBytes...)
	packet = append(packet, []byte("\x00\x04MQTT\x04\xc2\x00\x3c\x00\x08mqttinfo")...)
	packet = append(packet, usernameLenBytes...)
	packet = append(packet, []byte(b.Username)...)
	packet = append(packet, pwdLenBytes...)
	packet = append(packet, []byte(b.Password)...)

	return packet
}

// getConnectV5 ... TODO
func (b *BrokerInfo) getConnectV5() []byte {

	if b.Username == "" {
		return []byte(connectV5)
	}

	// Length were checked in main()
	usernameLen := uint16(len(b.Username))
	pwdLen := uint16(len(b.Password))

	remainingLen := 25 + int(usernameLen+pwdLen)

	remainingLenBytes := encodeLength(remainingLen)

	packet := make([]byte, 1)
	packet[0] = 0x10

	usernameLenBytes := make([]byte, 2)
	pwdLenBytes := make([]byte, 2)

	binary.BigEndian.PutUint16(usernameLenBytes, usernameLen)
	binary.BigEndian.PutUint16(pwdLenBytes, pwdLen)

	packet = append(packet, remainingLenBytes...)
	packet = append(packet, []byte("\x00\x04MQTT\x05\xc2\x00\x3c\x00\x08mqttinfo")...)
	packet = append(packet, usernameLenBytes...)
	packet = append(packet, []byte(b.Username)...)
	packet = append(packet, pwdLenBytes...)
	packet = append(packet, []byte(b.Password)...)

	return packet
}

func (b *BrokerInfo) getServer() string {
	return fmt.Sprintf("%v:%v", b.Host, b.Port)
}

// connects to the broker, either anonymously or with creds
func (b *BrokerInfo) connectV4() (net.Conn, error) {

	conn, err := net.DialTimeout("tcp", b.getServer(), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("TCP connection failed: %v", err)
	}

	err = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	if err != nil {
		return nil, err
	}

	conn.Write(b.getConnectV4())
	connack := make([]byte, 100)
	_, err = conn.Read(connack)
	if err != nil {
		return nil, fmt.Errorf("CONNACK read failed: %v", err)
	}

	if connack[3] != 0x00 {
		return nil, fmt.Errorf("Connection request rejected (code %v)", connack[3])
	}

	return conn, nil
}

// connects to the broker, either anonymously or with creds
func (b *BrokerInfo) connectV5() (net.Conn, error) {

	conn, err := net.DialTimeout("tcp", b.getServer(), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("TCP connection failed: %v", err)
	}

	err = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	if err != nil {
		return nil, err
	}

	conn.Write(b.getConnectV5())
	connack := make([]byte, 100)
	_, err = conn.Read(connack)
	if err != nil {
		return nil, fmt.Errorf("CONNACK read failed: %v", err)
	}

	if connack[3] != 0x00 {
		return nil, fmt.Errorf("Connection request rejected (code %v)", connack[3])
	}

	return conn, nil
}

// check if broker responds to ping, to know if we're connected
func pingsBack(conn net.Conn) bool {
	conn.Write([]byte(pingreq))
	resp := make([]byte, 100)
	bytes, err := conn.Read(resp)
	if err != nil {
		return false
	}
	if string(resp[:bytes]) != pingresp {
		return false
	}
	return true
}

// TODO: use HasPrefix
// AnalyzeV4 ...
func (b *BrokerInfo) AnalyzeV4() error {

	conn, err := b.connectV4()
	if err != nil {
		return err
	}
	defer conn.Close()

	// Check response to ping
	if !pingsBack(conn) {
		return fmt.Errorf("Broker does not respond to PINGREQ")
	}

	// Check QoS 1 support by checking PUBACK
	conn.Write([]byte(publishV4Q1))
	puback := make([]byte, 100)
	_, err = conn.Read(puback)
	if err == nil {
		// Check PUBACK's value including message id
		if strings.HasPrefix(string(puback), pubackV4Q1) {
			b.V4QoS1 = true
		}
	}

	// Attempts to reconnect if needed
	if !pingsBack(conn) {
		conn, err = b.connectV4()
		if err != nil {
			return err
		}
	}

	// Check QoS 2 support by checking PUBREC
	conn.Write([]byte(publishV4Q2))
	pubrec := make([]byte, 100)
	_, err = conn.Read(pubrec)
	if err == nil {
		// Check PUBREL and PUBCOMP values
		if strings.HasPrefix(string(pubrec), pubrecV4Q2) {
			conn.Write([]byte(pubrelV4Q2))
			pubcomp := make([]byte, 100)
			_, err = conn.Read(pubcomp)
			if err == nil {
				if strings.HasPrefix(string(pubcomp), pubcompV4Q2) {
					b.V4QoS2 = true
				}
			}
		}
	}

	if !pingsBack(conn) {
		conn, err = b.connectV4()
		if err != nil {
			return err
		}
	}

	conn.Write([]byte(publishV4Q3))
	_, err = conn.Read(puback)
	if err == nil {
		// TODO: check for a puback or pubrel
		b.V4QoS3Response = true
	}

	if !pingsBack(conn) {
		conn, err = b.connectV4()
		if err != nil {
			return err
		}
	}

	// Check wildcard subscription
	conn.Write([]byte(subAllV4Q0))
	suback := make([]byte, 100)
	_, err = conn.Read(suback)
	if err == nil {
		// Check SUBACK's value
		if strings.HasPrefix(string(suback), subackV4Q0) {
			b.V4SubscribeAll = true
		}
	}

	conn.Close()

	conn, err = b.connectV4()
	if err != nil {
		return err
	}

	// Check invalid topic names support
	conn.Write([]byte(subInvalidV4Q0))
	suback = make([]byte, 100)
	_, err = conn.Read(suback)
	if err == nil {
		if strings.HasPrefix(string(suback), subackV4Q0) {
			b.V4InvalidTopics = true
		}
	}

	if !pingsBack(conn) {
		conn, err = b.connectV4()
		if err != nil {
			return err
		}
	}

	// Check invalid UTF8 topic names support
	conn.Write([]byte(subInvalidUTF8V4Q0))
	suback = make([]byte, 100)
	_, err = conn.Read(suback)
	if err == nil {
		if strings.HasPrefix(string(suback), subackV4Q0) {
			b.V4InvalidUTF8Topic = true
		}
	}

	if !pingsBack(conn) {
		conn, err = b.connectV4()
		if err != nil {
			return err
		}
	}

	// Check $SYS publication
	conn.Write([]byte(pubSysV4Q1))
	_, err = conn.Read(puback)
	if err == nil {
		if strings.HasPrefix(string(puback), pubackV4Q1) {
			b.V4PublishSYS = true
		}
	}

	// Need to close and reconnect to receive published message
	conn.Close()

	conn, err = b.connectV4()
	if err != nil {
		return err
	}

	// Check if $SYS messages are filtered or forwarded,
	// based on previous message that had the retain flag
	if b.V4PublishSYS {
		conn.Write([]byte(subSysAV4Q0))
		time.Sleep(1 * time.Second)
		_, err = conn.Read(suback)
		if err == nil {
			if strings.HasPrefix(string(suback), subackV4Q1+pubSysV4Q1) {
				b.V4FilterSYS = false
			}
		}
	}

	return nil
}

// AnalyzeV5 ...
func (b *BrokerInfo) AnalyzeV5() error {

	conn, err := b.connectV5()
	if err != nil {
		return err
	}
	defer conn.Close()

	// Check response to ping
	if !pingsBack(conn) {
		return fmt.Errorf("Broker does not respond to PINGREQ")
	}

	// TODO: support non-empty properties in puback?
	// TODO: figure out why mosquitto/hivemq add 8 random bytes
	// Check QoS 1 support by checking PUBACK
	conn.Write([]byte(publishV5Q1))
	puback := make([]byte, 100)
	_, err = conn.Read(puback)
	if err == nil {
		// Check PUBACK's value including message id
		// Case of publish accepted, or accepted but not matching subscriber
		s := string(puback)
		if strings.HasPrefix(s, pubackV5Q1a) ||
			strings.HasPrefix(s, pubackV5Q1b) {
			b.V5QoS1 = true
		}
	}

	// Attempts to reconnect if needed
	if !pingsBack(conn) {
		conn, err = b.connectV5()
		if err != nil {
			return err
		}
	}

	// Check QoS 2 support by checking PUBREC
	conn.Write([]byte(publishV5Q2))
	pubrec := make([]byte, 100)
	_, err = conn.Read(pubrec)
	if err == nil {
		s := string(pubrec)
		if strings.HasPrefix(s, pubrecV5Q2a) ||
			strings.HasPrefix(s, pubrecV5Q2b) {
			conn.Write([]byte(pubrelV5Q2))
			pubcomp := make([]byte, 100)
			_, err = conn.Read(pubcomp)
			if err == nil && strings.HasPrefix(string(pubcomp), pubcompV5Q2) {
				b.V5QoS2 = true
			}
		}
	}

	if !pingsBack(conn) {
		conn, err = b.connectV5()
		if err != nil {
			return err
		}
	}

	// TODO: broker may respond with an error even if not supported
	conn.Write([]byte(publishV5Q3))
	_, err = conn.Read(puback)
	if err == nil {
		s := string(puback)
		if strings.HasPrefix(s, pubackV5Q1a) ||
			strings.HasPrefix(s, pubackV5Q1b) ||
			strings.HasPrefix(s, pubrecV5Q2a) ||
			strings.HasPrefix(s, pubrecV5Q2b) {
			b.V5QoS3Response = true
		}
	}

	if !pingsBack(conn) {
		conn, err = b.connectV5()
		if err != nil {
			return err
		}
	}

	// Check wildcard subscription
	conn.Write([]byte(subAllV5Q0))
	suback := make([]byte, 100)
	_, err = conn.Read(suback)
	if err == nil {
		// Check SUBACK's value
		if strings.HasPrefix(string(suback), subackV5Q0) {
			b.V5SubscribeAll = true
		}
	}

	conn.Close()

	conn, err = b.connectV5()
	if err != nil {
		return err
	}

	// Check invalid topic names support
	conn.Write([]byte(subInvalidV5Q0))
	suback = make([]byte, 100)
	_, err = conn.Read(suback)
	if err == nil {
		if strings.HasPrefix(string(suback), subackV5Q0) {
			b.V5InvalidTopics = true
		}
	}

	if !pingsBack(conn) {
		conn, err = b.connectV5()
		if err != nil {
			return err
		}
	}

	// Check invalid UTF8 topic names support
	conn.Write([]byte(subInvalidUTF8V5Q0))
	suback = make([]byte, 100)
	_, err = conn.Read(suback)
	if err == nil {
		if strings.HasPrefix(string(suback), subackV5Q0) {
			b.V5InvalidUTF8Topic = true
		}
	}

	if !pingsBack(conn) {
		conn, err = b.connectV5()
		if err != nil {
			return err
		}
	}

	// Check $SYS publication
	conn.Write([]byte(pubSysV5Q1))
	_, err = conn.Read(puback)
	if err == nil {
		s := string(puback)
		if strings.HasPrefix(s, pubackV5Q1a) ||
			strings.HasPrefix(s, pubackV5Q1b) {
			b.V5PublishSYS = true
		}
	}

	// Need to close and reconnect to receive published message
	conn.Close()

	conn, err = b.connectV5()
	if err != nil {
		return err
	}

	// Check if $SYS messages are filtered or forwarded,
	// based on previous message that had the retain flag
	if b.V5PublishSYS {
		conn.Write([]byte(subSysAV5Q0))
		time.Sleep(1 * time.Second)
		_, err = conn.Read(suback)
		if err == nil {
			if strings.HasPrefix(string(suback), subackV5Q1+pubSysV5Q1) {
				b.V5FilterSYS = false
			}
		}
	}

	return nil
}

// TODO: use HasPrefix
// GuessBroker attempts to determine the broker software
// Must be run after AnalyzeV4()
func (b *BrokerInfo) GuessBroker() error {

	conn, err := b.connectV4()
	if err != nil {
		return err
	}

	// Are $SYS messages sent at all?
	conn.Write([]byte(subSysAllV4Q0))
	suback := make([]byte, 100)
	bytes, err := conn.Read(suback)
	if err == nil {
		if bytes >= len(subackV4Q0) {
			if string(suback[:len(subackV4Q0)]) == subackV4Q0 {
				messages := make([]byte, 100)
				bytes, err = conn.Read(messages)
				if bytes == 0 {
					// HiveMQ likely to not sent $SYS messages
					b.TypeGuessed = "HiveMQ"
					return nil
				}
			}
		}
	}

	conn.Close()
	conn, err = b.connectV4()
	if err != nil {
		return err
	}

	// Receive something on (say) $SYS/+/router/subscriptions?
	// Then VerneMQ most likely
	conn.Write([]byte(subSysVerneV4Q0))
	bytes, err = conn.Read(suback)
	if err == nil {
		if bytes >= len(subackV4Q0) {
			if string(suback[:len(subackV4Q0)]) == subackV4Q0 {
				messages := make([]byte, 100)
				bytes, err = conn.Read(messages)
				if bytes != 0 {
					// VerneMQ seems to be the only one to use this topics
					b.TypeGuessed = "VerneMQ"
					return nil
				}
			}
		}
	}

	conn.Close()
	conn, err = b.connectV4()
	if err != nil {
		return err
	}

	// Receive sth on (say) $SYS/+/load/messages/sent/+ ?
	// Then mosquitto most likely
	conn.Write([]byte(subSysMosqV4Q0))
	bytes, err = conn.Read(suback)
	if err == nil {
		if bytes >= len(subackV4Q0) {
			if string(suback[:len(subackV4Q0)]) == subackV4Q0 {
				messages := make([]byte, 100)
				bytes, err = conn.Read(messages)
				if bytes != 0 {
					// This topic is supported by mosquitto, potentially others who follow its $SYS syntax
					if b.V4PublishSYS {
						b.TypeGuessed = "mosquitto"
					}
					return nil
				}
			}
		}
	}

	if !b.V4PublishSYS {
		b.TypeGuessed = "HiveMQ"
	}

	return nil
}

// CheckConnectionV4 determines if v3.1.1 is supported
func (b *BrokerInfo) CheckConnectionV4() error {

	server := fmt.Sprintf("%v:%v", b.Host, b.Port)
	conn, err := net.DialTimeout("tcp", server, 10*time.Second)
	if err != nil {
		return fmt.Errorf("Dial to %v failed", server)
	}
	defer conn.Close()
	conn.Write([]byte(connectV4))
	connack := make([]byte, 100)
	bytes, err := conn.Read(connack)
	if err != nil {
		return fmt.Errorf("CONNACK read failed: %v", err)
	}

	// CONNACK should be at least 5 bytes if V5.0 supported,
	// but some v3.1.1 protocol seem to return longer than 4 bytes CONNACKs
	if bytes < 4 {
		return fmt.Errorf("CONNACK too short (%v bytes)", bytes)
	}

	reasonCode := connack[3]

	// Defaults to a server speaking V3.1.1, will be changed if needed
	b.V4 = true
	b.V4Anonymous = false

	// Distinguish errors indicated lack of connectivity from errors
	// indicating an error preventing connectivity check;
	// return an error for the former, set attributes otherwise
	// Note: Assumes that server returns "Unsupported protocol version"
	// if does not support protocol version and credentials invalid
	switch reasonCode {
	case 0x00:
		b.V4Anonymous = true
		return nil
	case 0x01:
		return fmt.Errorf("CONNACK: Protocol version not supported")
	case 0x02:
		return fmt.Errorf("CONNACK: Identifier rejected")
	case 0x03:
		return fmt.Errorf("CONNACK: Server unavailable")
	case 0x04:
		// Bad username of password
		return nil
	case 0x05:
		// Connection refused
		return nil
	default:
		// Server may not speak v3.1.1
		b.V4 = false
		return fmt.Errorf("CONNACK: Unknown reason code")

	}
}

// CheckConnectionV5 determines if v5.0 is supported
func (b *BrokerInfo) CheckConnectionV5() error {

	server := fmt.Sprintf("%v:%v", b.Host, b.Port)
	conn, err := net.DialTimeout("tcp", server, 10*time.Second)
	if err != nil {
		return fmt.Errorf("Dial to %v failed", server)
	}
	defer conn.Close()
	conn.Write([]byte(connectV5))
	connack := make([]byte, 100)
	bytes, err := conn.Read(connack)
	if err != nil {
		return fmt.Errorf("CONNACK read failed: %v", err)
	}

	// CONNACK should be at least 5 bytes if V5.0 supported,
	// but some v3.1.1 protocol seem to return longer than 4 bytes CONNACKs
	if bytes < 4 {
		return fmt.Errorf("CONNACK too short (%v bytes)", bytes)
	}

	reasonCode := connack[3]

	// Defaults to a server speaking V5.0, will be changed if needed
	b.V5 = true
	b.V5Anonymous = false

	// Distinguish errors indicated lack of connectivity from errors
	// indicating an error preventing connectivity check;
	// return an error for the former, set attributes otherwise
	// Note: Assumes that server returns "Unsupported protocol version"
	// if does not support protocol version and credentials invalid
	switch reasonCode {
	case 0x00:
		b.V5Anonymous = true
		return nil
	case 0x01:
		// V3.1.1's Unacceptable protocol version
		b.V5 = false
		return nil
	case 0x05:
		// V3.1.1's Connection refused (won't parse V5.0 packet)
		b.V5 = false
		return nil
	case 0x80:
		return fmt.Errorf("CONNACK: Unspecified error")
	case 0x81:
		return fmt.Errorf("CONNACK: Malformed packet")
	case 0x82:
		return fmt.Errorf("CONNACK: Protocol error")
	case 0x83:
		return fmt.Errorf("CONNACK: Implementation specific error")
	case 0x84:
		// Unsupported protocol version (as per V5.0)
		// means that server speaks V5.0 but doesnt like the version received
		b.V5 = false
		return nil
	case 0x85:
		return fmt.Errorf("CONNACK: Client identifier not valid")
	case 0x86:
		// Bad User Name or Password
		return nil
	case 0x87:
		// Not authorized
		return nil
	case 0x88:
		return fmt.Errorf("CONNACK: Server unavailable")
	case 0x89:
		return fmt.Errorf("CONNACK: Server busy")
	case 0x8a:
		return fmt.Errorf("CONNACK: Banned")
	case 0x8c:
		// Bad authentication method
		return nil
	case 0x90:
		return fmt.Errorf("CONNACK: Topic name invalid")
	case 0x95:
		return fmt.Errorf("CONNACK: Packet too large")
	case 0x97:
		return fmt.Errorf("CONNACK: Quota exceeded")
	case 0x9a:
		return fmt.Errorf("CONNACK: Retain not supported")
	case 0x9b:
		return fmt.Errorf("CONNACK: QoS not supported")
	case 0x9c:
		return fmt.Errorf("CONNACK: Use another server")
	case 0x9d:
		return fmt.Errorf("CONNACK: Server moved")
	case 0x9f:
		return fmt.Errorf("CONNACK: Connection rate exceeded")
	default:
		// server likely may not speak v5.0
		b.V5 = false
		return nil

	}
}
