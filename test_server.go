package main

import (
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"strings"
	"os"
	"os/signal"
	"time"
)


//////////////////////////////
// Chat Server protocol
// REG <ascii 4 bytes>[ <name>]
// SND <ascii 4 bytes>[ <name of recipient>][ <msg>]
// RCV <ascii 4 bytes>[ <name of sender>][ <ascii 4 bytes word count>][ <msg>]
// BYE 0000
// Examples
// Client -> Server
// REG 0008 CHIRAYU
// SND 0018 VENKY HELLO WORLD
// Server -> Client
// RCV 0025 CHIRAYU 0002 HELLO WORLD
// BYE 0000
//////////////////////////////


type ChatServer struct {
	clients map[string](*net.TCPConn)
	mutex sync.Mutex
	test_mode bool

	ch chan bool
	waitGroup *sync.WaitGroup
}

type ChatClient struct {
 	name string
 	conn *net.TCPConn
	rx int
	tx int
}

type Message struct {
	command string
	payload_len int
	payload string
}

func NewChatServer() *ChatServer {
	return &ChatServer{clients : make(map[string](*net.TCPConn)), mutex: sync.Mutex{}, test_mode: false, ch: make(chan bool), waitGroup: &sync.WaitGroup{}}
}

func (server *ChatServer) Serve(listener *net.TCPListener) {
	log.Println("Server started")
	for {
		// select {
		// case <-server.ch:
		// 	log.Println("Stopping listening on", listener.Addr())
		// 	listener.Close()
		// 	return
		// default:
		// }

		listener.SetDeadline(time.Now().Add(800 * time.Millisecond))
		conn, err := listener.AcceptTCP()

		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			log.Println("Listen Error -->", err)
		}

		log.Println(conn.RemoteAddr(), "connected")

		server.waitGroup.Add(1)
		go server.ConnectionHandler(&ChatClient{conn:conn})
	}

}

func (server *ChatServer) addClient(client *ChatClient) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	server.clients[client.name] = client.conn
}

func (server *ChatServer) removeClient(client *ChatClient) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	if server.clients[client.name] == client.conn {
		delete(server.clients, client.name)
	}
}

func (server *ChatServer) getConnection(name string) *net.TCPConn {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	connection :=server.clients[name]
	return connection
}


func (server *ChatServer) Stop() {
	close(server.ch)
	server.waitGroup.Wait()
}

func (server *ChatServer) ShutDownClient(client *ChatClient) {
	_, err := io.WriteString(client.conn, "BYE 0000")
	if err != nil {
		log.Println("Write Error -->", err)
	}
	server.removeClient(client)
	client.conn.Close()
	log.Println("Closed connection to:", client.name, "Tx", client.tx, "Rx", client.rx)
}

func (server *ChatServer) ignoreCount() int {
	if server.test_mode == true {
		return 1
	}
	return 0
}


func (server *ChatServer) ConnectionHandler(client *ChatClient) {

	defer server.ShutDownClient(client)
	defer server.waitGroup.Done()

	var msg_buf = make([]byte, 0)
	var tmp_buf = make([]byte, 128)

	for {
		select {
		case <-server.ch:
			log.Println("disconnecting", client.conn.RemoteAddr())
			return
		default:
		}

		client.conn.SetReadDeadline(time.Now().Add(10000 * time.Millisecond))
		read_len, err := client.conn.Read(tmp_buf)
		//log.Println("---->", read_len, string(tmp_buf))
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			log.Println("Read error -->", err)
			return
		}

		// append the buffer to the msg_buf
		result := make([]byte, len(msg_buf)+read_len)
		copy(result, msg_buf)
		copy(result[len(msg_buf):], tmp_buf)
		msg_buf = result

		//log.Print("\tMessage:", msg_buf, "--\n")

		for ;len(msg_buf) >= 8; {
			var msg *Message
			//log.Println(client.name, "Msg Len", len(msg_buf))
			
			// we have both the commnd and the length
			s := string(msg_buf[4:8])
			payload_len, err := strconv.Atoi(s)
			if err != nil {
				log.Println("Atoi failed", err)
				return
			}

			if len(msg_buf) < 8+payload_len {
				// the full buffer hasn't been read
				break;
			}

			msg = &Message{command: string(msg_buf[:3]),
				payload_len: payload_len,
				payload: string(msg_buf[9 : 8+payload_len]),
			}
			//log.Println("\tParsed Message:", msg)
			
			// keep the remaining buffer around to construct the next command
			msg_buf = msg_buf[8+payload_len+server.ignoreCount():]
			
			client.rx += 1
			err = server.processCommand(client, msg)
			if err != nil {
				log.Println("Invalid command", err)
				return
			}
		}
	}

}

func (server *ChatServer) processCommand(client *ChatClient, msg *Message) (err error) {
	switch msg.command {
	case "REG":
		client.name = msg.payload
		server.addClient(client)
		log.Println("Registered:", msg.payload, "Total Clients:", len(server.clients))
	case "SND":
		var split_payload []string
		//log.Println("Received:", msg.payload)
		split_payload = strings.SplitN(msg.payload, " ", 2)
		receipient := server.getConnection(split_payload[0])

		if receipient != nil {
			word_count := strconv.Itoa(len(strings.Split(split_payload[1], " ")))
			for len(word_count) < 4 {
				word_count = "0" + word_count
			}

			payload := " " + client.name + " " + word_count + " " + split_payload[1]
			payload_len := string(strconv.Itoa(len(payload)))

			for len(payload_len) < 4{
				payload_len = "0" + payload_len
			}
			// ignore messages with len larger than 9999
			if len(payload_len) == 4{
				_, err = receipient.Write([]byte("RCV " + payload_len + payload))
				if err != nil {
					log.Println("Write failed -->", err)
					return nil
				}
				client.tx += 1
			}else{
				log.Println("ERROR. Long message")
				return nil
			}
		}
	default:
		return errors.New("Unsupported command")
	}
	return nil
}


const listenAddr = "0.0.0.0:4000"

func main() {

	laddr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if nil != err {
		log.Fatalln(err)
	}

	listener, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatal(err)
	}

	server := NewChatServer()
	go server.Serve(listener)

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt)
	<-signalChannel
	log.Println("Exiting.... ")
	server.Stop()
}

