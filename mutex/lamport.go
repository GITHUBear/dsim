package mutex

import (
	"dsim/denv"
	"fmt"
	"log"
	"sort"
	"sync"
)

type logicClock struct {
	timestamp uint64
	no        uint64
}

func (lc logicClock) Less(other logicClock) bool {
	if lc.timestamp == other.timestamp {
		return lc.no < other.no
	}
	return lc.timestamp < other.timestamp
}

type logicClocks []logicClock

func (lcs logicClocks) Len() int {
	return len(lcs)
}

func (lcs logicClocks) Swap(i, j int) {
	lcs[i], lcs[j] = lcs[j], lcs[i]
}

func (lcs logicClocks) Less(i, j int) bool {
	return lcs[i].Less(lcs[j])
}

type LamportState uint64

const (
	Free       LamportState = 1
	Requesting LamportState = 2
	Executing  LamportState = 3
)

type LamportMutex struct {
	mu LamportMutexMsg
	cb chan struct{}
}

type LamportNode struct {
	state LamportState
	logicClock
	reqClock    logicClock
	clusterSize uint64
	rq          logicClocks

	sender   chan<- denv.Msg
	receiver <-chan denv.Msg

	apiCh    chan LamportMutex // mutex request
	replyMap map[uint64]bool
	cb       chan struct{}

	closeCh chan struct{} // shutdown
	wg      sync.WaitGroup
}

type MsgType int32

const (
	MsgtypeRequest MsgType = 1
	MsgtypeReply   MsgType = 2
	MsgtypeRelease MsgType = 3
)

type LamportMsg struct {
	from    uint64
	to      uint64
	msgType MsgType
	msgTs   logicClock
	reqTs   logicClock
}

func getMsgTypeString(msgType MsgType) string {
	switch msgType {
	case MsgtypeRequest:
		return "REQUEST"
	case MsgtypeReply:
		return "REPLY"
	case MsgtypeRelease:
		return "RELEASE"
	default:
		log.Panic("wrong MsgType")
	}
	return ""
}

func (m LamportMsg) From() uint64 {
	return m.from
}

func (m LamportMsg) To() uint64 {
	return m.to
}

func (m LamportMsg) String() string {
	common := fmt.Sprintf("msgType: %v, from: %v, to: %v, ts: [%v, %v]",
		getMsgTypeString(m.msgType), m.from, m.to, m.msgTs.timestamp, m.msgTs.no)
	switch m.msgType {
	case MsgtypeReply:
		return fmt.Sprintf("{ %v }", common)
	default:
		return fmt.Sprintf("{ %v, reqTs: [%v, %v] }", common, m.reqTs.timestamp, m.reqTs.no)
	}
}

type MutexType int32

const (
	Lock   MutexType = 1
	Unlock MutexType = 2
)

type LamportMutexMsg struct {
	mutexType MutexType
}

func NewLamportNode(no uint64, clusterSize uint64) *LamportNode {
	wg := sync.WaitGroup{}
	wg.Add(1)
	return &LamportNode{
		state:       Free,
		logicClock:  logicClock{0, no},
		reqClock:    logicClock{0, no},
		clusterSize: clusterSize,

		rq:      []logicClock{},
		apiCh:   make(chan LamportMutex, 1024),
		closeCh: make(chan struct{}, 1024),
		wg:      wg,
	}
}

func (ln *LamportNode) RequestCriticalSection() {
	log.Printf("Node %v request for Critial Section", ln.no)
	ln.timestamp += 1
	ln.reqClock = ln.logicClock
	ln.rq = append(ln.rq, ln.reqClock)
	sort.Sort(ln.rq)
	for i := 0; i < int(ln.clusterSize); i++ {
		if i != int(ln.no) {
			req := LamportMsg{
				from:    ln.no,
				to:      uint64(i),
				msgType: MsgtypeRequest,
				msgTs:   ln.logicClock,
				reqTs:   ln.reqClock,
			}
			ln.sender <- req
			log.Printf("Node %v send REQUEST to Node %v: %v", ln.no, i, req.String())
		}
	}
	ln.state = Requesting
	ln.replyMap = make(map[uint64]bool)
}

func (ln *LamportNode) HandleRequest(ts logicClock) {
	ln.rq = append(ln.rq, ts)

}

func (ln *LamportNode) ReleaseCriticalSection(cb chan struct{}) {
	log.Printf("Node %v release for Critial Section", ln.no)
	for i := 0; i < int(ln.clusterSize); i++ {
		if i != int(ln.no) {
			req := LamportMsg{
				from:    ln.no,
				to:      uint64(i),
				msgType: MsgtypeRelease,
				msgTs:   ln.logicClock,
				reqTs:   ln.reqClock,
			}
			ln.sender <- req
			log.Printf("Node %v send RELEASE to Node %v: %v", ln.no, i, req.String())
		}
	}
	ln.state = Free
	cb <- struct{}{}
}

func (ln *LamportNode) HandleReleaseRequest(ts logicClock) {
	find := false
	idx := 0
	for i, clk := range ln.rq {
		if clk.timestamp == ts.timestamp && clk.no == ts.no {
			if find {
				log.Panic("Duplicated Request is not allowed")
				return
			}
			find = true
			idx = i
		}
	}
	if !find {
		log.Panic("Request is not found in RQ")
		return
	}
	ln.rq = append(ln.rq[:idx], ln.rq[idx+1:]...)
}

func (ln *LamportNode) SendReply(to uint64) {
	reply := LamportMsg{
		from:    ln.no,
		to:      to,
		msgType: MsgtypeReply,
		msgTs:   ln.logicClock,
	}
	ln.sender <- reply
	log.Printf("Node %v send REPLY to Node %v: %v", ln.no, to, reply.String())
}

// state: REQUESTING
func (ln *LamportNode) UpdateReplyMap(ts logicClock) {
	if ln.state != Requesting {
		log.Panicf("UpdateReplyMap while Node %v is not Requesting", ln.no)
		return
	}
	if ln.reqClock.Less(ts) {
		ln.replyMap[ts.no] = true
	}
	sort.Sort(ln.rq)
	log.Printf("Node %v After Update Reply Map: %v", ln.no, ln.rq)
	if uint64(len(ln.replyMap)) == ln.clusterSize-1 &&
		ln.rq[0].timestamp == ln.reqClock.timestamp && ln.rq[0].no == ln.reqClock.no {
		ln.state = Executing
		ln.cb <- struct{}{}
	}
}

func (ln *LamportNode) Start() {
	go func() {
		defer ln.wg.Done()
		log.Printf("Node %v start!", ln.no)
		for {
			stop := false

			select {
			case msg := <-ln.apiCh:
				switch msg.mu.mutexType {
				case Lock:
					if ln.state != Free {
						log.Panicf("Request for critical section while Node %v is not Free!", ln.no)
					}
					ln.RequestCriticalSection()
					ln.cb = msg.cb
				case Unlock:
					if ln.state != Executing {
						log.Panicf("Unlock critical section while Node %v is not Executing!", ln.no)
					}
					ln.HandleReleaseRequest(ln.reqClock)
					ln.ReleaseCriticalSection(msg.cb)
				}
			case msg := <-ln.receiver:
				lmsg, ok := msg.(LamportMsg)
				if !ok {
					log.Panic("Wrong Msg!")
				}
				// Update logical clock
				ln.timestamp = denv.Max64(ln.timestamp, lmsg.msgTs.timestamp) + 1
				switch ln.state {
				case Free:
					if lmsg.msgType == MsgtypeRequest {
						ln.HandleRequest(lmsg.reqTs)
						ln.SendReply(lmsg.from)
					} else if lmsg.msgType == MsgtypeRelease {
						ln.HandleReleaseRequest(lmsg.reqTs)
					}
				case Requesting:
					if lmsg.msgType == MsgtypeRequest {
						ln.HandleRequest(lmsg.reqTs)
						ln.SendReply(lmsg.from)
					} else if lmsg.msgType == MsgtypeRelease {
						ln.HandleReleaseRequest(lmsg.reqTs)
					}
					ln.UpdateReplyMap(lmsg.msgTs)
				case Executing:
					if lmsg.msgType == MsgtypeRequest {
						ln.HandleRequest(lmsg.reqTs)
					} else if lmsg.msgType == MsgtypeRelease {
						log.Panicf("Get Msg Release while Executing on Node %v!", ln.no)
					}
				}
			case <-ln.closeCh:
				stop = true
			}
			if stop {
				break
			}
		}
	}()
}

func (ln *LamportNode) Stop() {
	ln.closeCh <- struct{}{}
	ln.wg.Wait()
	log.Printf("Node %v stop!", ln.no)
}

func (ln *LamportNode) Lock(cb chan struct{}) {
	lmu := LamportMutex{
		mu: LamportMutexMsg{Lock},
		cb: cb,
	}
	ln.apiCh <- lmu
}

func (ln *LamportNode) Unlock(cb chan struct{}) {
	lmu := LamportMutex{
		mu: LamportMutexMsg{Unlock},
		cb: cb,
	}
	ln.apiCh <- lmu
}
