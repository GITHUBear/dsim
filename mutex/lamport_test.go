package mutex

import (
	"dsim/denv"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	opt := denv.DefaultEnvOptions()
	net := denv.NewFIFONetwork(opt)
	n := opt.ClusterSize
	var nodes []*LamportNode
	for i := 0; i < int(n); i++ {
		node := NewLamportNode(uint64(i), n)
		nodes = append(nodes, node)
		node.receiver, node.sender = net.AddNode(uint64(i))
		node.Start()
	}

	cb := make(chan struct{})
	nodes[0].Lock(cb)
	<-cb
	log.Printf("Mutex Hold!")
	cb2 := make(chan struct{})
	nodes[0].Unlock(cb2)
	<-cb2
	log.Printf("Mutex Release!")

	time.Sleep(1000 * time.Microsecond)
	net.Stop()
	for _, node := range nodes {
		node.Stop()
	}
}

func TestConcurrency2(t *testing.T) {
	opt := denv.DefaultEnvOptions()
	net := denv.NewFIFONetwork(opt)
	n := opt.ClusterSize
	var nodes []*LamportNode
	for i := 0; i < int(n); i++ {
		node := NewLamportNode(uint64(i), n)
		nodes = append(nodes, node)
		node.receiver, node.sender = net.AddNode(uint64(i))
		node.Start()
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for i := 1; i <= 5; i++ {
			cb := make(chan struct{})
			nodes[1].Lock(cb)
			<-cb
			log.Printf("Node 1 Mutex %v Hold!", i)

			delay := 100 + rand.Intn(20)
			time.Sleep(time.Duration(delay) * time.Microsecond)

			cb2 := make(chan struct{})
			nodes[1].Unlock(cb2)
			<-cb2
			log.Printf("Node 1 Mutex %v Release!", i)
		}
		wg.Done()
	}()

	for i := 1; i <= 5; i++ {
		cb := make(chan struct{})
		nodes[0].Lock(cb)
		<-cb
		log.Printf("Node 0 Mutex %v Hold!", i)

		delay := 100 + rand.Intn(20)
		time.Sleep(time.Duration(delay) * time.Microsecond)

		cb2 := make(chan struct{})
		nodes[0].Unlock(cb2)
		<-cb2
		log.Printf("Node 0 Mutex %v Release!", i)
	}

	time.Sleep(3000 * time.Microsecond)
	wg.Wait()

	net.Stop()
	for _, node := range nodes {
		node.Stop()
	}
}

func TestConcurrency3(t *testing.T) {
	opt := denv.DefaultEnvOptions()
	opt.ClusterSize = 5
	net := denv.NewFIFONetwork(opt)
	n := opt.ClusterSize
	var nodes []*LamportNode
	for i := 0; i < int(n); i++ {
		node := NewLamportNode(uint64(i), n)
		nodes = append(nodes, node)
		node.receiver, node.sender = net.AddNode(uint64(i))
		node.Start()
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := 1; i <= 2; i++ {
			cb := make(chan struct{})
			nodes[1].Lock(cb)
			<-cb
			log.Printf("Node 1 Mutex %v Hold!", i)

			delay := 100 + rand.Intn(20)
			time.Sleep(time.Duration(delay) * time.Microsecond)

			cb2 := make(chan struct{})
			nodes[1].Unlock(cb2)
			<-cb2
			log.Printf("Node 1 Mutex %v Release!", i)
		}
		wg.Done()
	}()

	go func() {
		for i := 1; i <= 2; i++ {
			cb := make(chan struct{})
			nodes[2].Lock(cb)
			<-cb
			log.Printf("Node 2 Mutex %v Hold!", i)

			delay := 100 + rand.Intn(20)
			time.Sleep(time.Duration(delay) * time.Microsecond)

			cb2 := make(chan struct{})
			nodes[2].Unlock(cb2)
			<-cb2
			log.Printf("Node 2 Mutex %v Release!", i)
		}
		wg.Done()
	}()

	for i := 1; i <= 2; i++ {
		cb := make(chan struct{})
		nodes[0].Lock(cb)
		<-cb
		log.Printf("Node 0 Mutex %v Hold!", i)

		delay := 100 + rand.Intn(20)
		time.Sleep(time.Duration(delay) * time.Microsecond)

		cb2 := make(chan struct{})
		nodes[0].Unlock(cb2)
		<-cb2
		log.Printf("Node 0 Mutex %v Release!", i)
	}

	time.Sleep(6000 * time.Microsecond)
	wg.Wait()

	net.Stop()
	for _, node := range nodes {
		node.Stop()
	}
}