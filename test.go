package main

import (
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
	"sync/atomic"
	"flag"
//	"math/rand"
)

type ChatTester struct {
	testlets int
	runTime  int
	finalWaitTime int
	serverAddr *net.TCPAddr

	burst uint64
	burstInterval uint64

	msgTx uint64
	msgRx uint64

	startXferCh chan bool
	stopCh chan bool

	regWaitGroup *sync.WaitGroup
	exitWaitGroup *sync.WaitGroup
	finalWaitGroup *sync.WaitGroup
}

func (tester *ChatTester) Register() {
	for i := 0; i < tester.testlets; i++ {
		tester.regWaitGroup.Add(1)
		tester.exitWaitGroup.Add(1)
		tester.finalWaitGroup.Add(1)
		go tester.TestLet(i)
	}
	tester.regWaitGroup.Wait()
	log.Println("Started Testlets", tester.testlets)
}

func generate_name(name string, number int, suffix_len int) string{
	suffix := strconv.Itoa(number)
	for len(suffix) < suffix_len {
		suffix = "0" + suffix
	}		
	return name + suffix
}


func (tester *ChatTester) TestLet(number int) {
	var tx uint64
	var rx uint64

	test_message := "Google doesn't shows off driverless car prototype"
	prefix := "BAT"
	suffix_len := 10
	// length calculation. example RCV 0025 CHIRAYU 0002 HELLO WORLD
	rcv_msg_len := 3 + 1 + 4 + 1 + len(prefix) + suffix_len + 1+ 4 + 1 + len(test_message)

	conn, err := net.DialTCP("tcp4", nil, tester.serverAddr)
	if err != nil {
		log.Fatal(err)
	}

	payload := " " + generate_name(prefix, number, suffix_len)

	plen := strconv.Itoa(len(payload))
	for len(plen) < 4 {
		plen = "0" + plen
	}

	io.WriteString(conn, "REG "+ plen + payload)

	tester.regWaitGroup.Done()

	<-tester.startXferCh

	go func () {
		for {
			for i := 0; i < tester.testlets; i++ {
				select {
				case <-tester.stopCh:
					return
				default:
				}

				if i == number {
					continue
				}
				
				rname := generate_name(prefix, i, suffix_len)
				payload := " " + rname + " " + test_message
				plen := strconv.Itoa(len(payload))
				for len(plen) < 4 {
					plen = "0" + plen
				}
				msg := "SND " + plen + payload
				_, err := io.WriteString(conn, msg)
				if err != nil {
					return
				}
				tx += 1
				if (tester.burst != 0) && (tx % tester.burst == 0) {
					//log.Println("Sleeping", tx, tester.burstInterval)
					time.Sleep(time.Duration(tester.burstInterval) * time.Millisecond)
				}
			}
		}
	}()

	go func() {
		var tmp_buf = make([]byte, rcv_msg_len)

		for {
			_, err := conn.Read(tmp_buf)
			//log.Println(string(tmp_buf))
			if err != nil {
				return
			}
			rx += 1
		}
	}()

	<-tester.stopCh

	atomic.AddUint64(&tester.msgTx, tx)
	atomic.AddUint64(&tester.msgRx, rx)
	tx = 0
	rx = 0

	tester.exitWaitGroup.Done()

	time.Sleep(time.Duration(tester.finalWaitTime) * time.Millisecond)
	atomic.AddUint64(&tester.msgTx, tx)
	atomic.AddUint64(&tester.msgRx, rx)

	conn.Close() //This will close the go routines for read and write

	tester.finalWaitGroup.Done()

}

func (tester *ChatTester) Start() {
	close(tester.startXferCh)
}

func (tester *ChatTester) Stop() {
	time.Sleep(time.Duration(tester.runTime) * time.Millisecond)
	close(tester.stopCh)
	tester.exitWaitGroup.Wait()
	log.Println("Tx", tester.msgTx, "in", tester.runTime, "ms")
	log.Println("Rx", tester.msgRx, "in", tester.runTime, "ms")
	log.Println("---------------")
	log.Println("Waiting to check for message loss", tester.finalWaitTime, "ms")
	tester.finalWaitGroup.Wait()
	log.Println("Final Tx", tester.msgTx)
	log.Println("Final Rx", tester.msgRx)
	log.Println("Message Loss", (int64)(tester.msgTx - tester.msgRx))
}

func main() {

	serverPtr := flag.String("server", "localhost", "server address")
	portPtr := flag.String("port", "4000", "server's port")
	clientsPtr := flag.Int("clients", 2, "number of clients")
	durationPtr := flag.Int("duration", 1000, "test duration (ms)")
	finalPtr := flag.Int("wait", 5000, "duration to wait for before checking for message loss (ms)")
	burstPtr := flag.Uint64("burst", 10, "Number of messages to send in one burst")
	burstIntervalPtr := flag.Uint64("burst_interval", 20, "Wait interval between bursts (ms)")

	flag.Parse()
	
	log.Println("---------------")
	log.Println("server:", *serverPtr)
	log.Println("port:", *portPtr)
	log.Println("clients:", *clientsPtr)
	log.Println("duration:", *durationPtr)
	log.Println("final wait:", *finalPtr)
	log.Println("burst:", *burstPtr)
	log.Println("burst interval:", *burstIntervalPtr)
	log.Println("---------------")


	serverAddr := *serverPtr+":"+*portPtr

	saddr, err := net.ResolveTCPAddr("tcp", serverAddr)
	if nil != err {
		log.Fatalln(err)
	}

	tester := ChatTester{testlets: *clientsPtr, runTime: *durationPtr, serverAddr: saddr,
		finalWaitTime: *finalPtr,
		burst: *burstPtr, burstInterval: *burstIntervalPtr, 
		startXferCh: make(chan bool), stopCh: make(chan bool), 
		regWaitGroup: &sync.WaitGroup{}, exitWaitGroup: &sync.WaitGroup{}, finalWaitGroup: &sync.WaitGroup{}}

	tester.Register()
	tester.Start()
	tester.Stop()
}
