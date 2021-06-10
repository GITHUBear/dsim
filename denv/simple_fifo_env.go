package denv

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type FIFONetwork struct {
	ins map[uint64]chan Msg
	outs map[uint64]chan Msg
	closeCh map[uint64]chan struct{}
	wg sync.WaitGroup
	opt *EnvOptions
}

func NewFIFONetwork(opt *EnvOptions) *FIFONetwork {
	ins := make(map[uint64]chan Msg)
	outs := make(map[uint64]chan Msg)
	for i := 0; i < int(opt.ClusterSize); i++ {
		ins[uint64(i)] = make(chan Msg, 1024)
		outs[uint64(i)] = make(chan Msg, 1024)
	}
	n := FIFONetwork{
		ins: ins,
		outs: outs,
		closeCh: make(map[uint64]chan struct{}),
		opt: opt,
	}
	n.wg = sync.WaitGroup{}
	n.wg.Add(int(opt.ClusterSize))
	return &n
}

func (n *FIFONetwork) AddNode(no uint64) (<-chan Msg, chan<- Msg) {
	ch := make(chan struct{}, 1024)
	n.closeCh[no] = ch
	go func(no uint64, inputCh chan Msg) {
		defer n.wg.Done()
		for {
			stop := false
			select {
			case msg := <-inputCh:
				if msg.From() != no {
					log.Panicf("msg.From %v is different from no: %v", msg.From(), no)
				}
				delay := n.opt.Delay + uint64(rand.Intn(int(n.opt.DelayRange)))
				time.Sleep(time.Duration(delay) * time.Microsecond)
				//log.Printf("Transport msg from Node %v to Node %v with delay %v ms",
				//	msg.From(), msg.To(), delay)
				n.outs[msg.To()] <- msg
			case <-ch:
				stop = true
			}
			if stop {
				log.Printf("tunnel %v stopped", no)
				break
			}
		}
	}(no, n.ins[no])
	return n.outs[no], n.ins[no]
}

func (n *FIFONetwork) Stop() {
	for _, ch := range n.closeCh {
		ch <- struct{}{}
	}
	n.wg.Wait()
}