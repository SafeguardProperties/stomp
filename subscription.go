package stomp

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/go-stomp/stomp/frame"
)

const (
	subStateActive  = 0
	subStateClosing = 1
	subStateClosed  = 2
)

// The Subscription type represents a client subscription to
// a destination. The subscription is created by calling Conn.Subscribe.
//
// Once a client has subscribed, it can receive messages from the C channel.
type Subscription struct {
	C           chan *Message
	id          string
	destination string
	conn        *Conn
	ackMode     AckMode
	state       int32
	closeChan   chan struct{}
}

// BUG(jpj): If the client does not read messages from the Subscription.C
// channel quickly enough, the client will stop reading messages from the
// server.

// Identification for this subscription. Unique among
// all subscriptions for the same Client.
func (s *Subscription) Id() string {
	return s.id
}

// Destination for which the subscription applies.
func (s *Subscription) Destination() string {
	return s.destination
}

// AckMode returns the Acknowledgement mode specified when the
// subscription was created.
func (s *Subscription) AckMode() AckMode {
	return s.ackMode
}

// Active returns whether the subscription is still active.
// Returns false if the subscription has been unsubscribed.
func (s *Subscription) Active() bool {
	return atomic.LoadInt32(&s.state) == subStateActive
}

// Unsubscribes and closes the channel C.
func (s *Subscription) Unsubscribe(opts ...func(*frame.Frame) error) error {
	// transition to the "closing" state
	if !atomic.CompareAndSwapInt32(&s.state, subStateActive, subStateClosing) {
		return ErrCompletedSubscription
	}

	f := frame.New(frame.UNSUBSCRIBE, frame.Id, s.id)

	for _, opt := range opts {
		if opt == nil {
			return ErrNilOption
		}
		err := opt(f)
		if err != nil {
			return err
		}
	}

	err := s.conn.sendFrame(f)
	if err != nil {
		log.Printf("failed to send frame in unsubscribe: %v", err)
	}

	// UNSUBSCRIBE is a bit weird in that it is tagged with a "receipt" header
	// on the I/O goroutine, so the above call to sendFrame() will not wait
	// for the resulting RECEIPT.
	//
	// We don't want to interfere with `s.C` since we might be "stealing"
	// MESSAGEs or ERRORs from another goroutine, so use a sync.Cond to
	// wait for the terminal state transition instead.
	timer := time.NewTimer(120 * time.Second)
	defer timer.Stop()
	select {
	case <-s.closeChan:
		return nil
		//log.Printf("Got the go ahead to close this subscription")
	case <-timer.C:
		log.Printf("timeout waiting for close")
		return ErrUnsubscribeTimeout
	}
}

// Read a message from the subscription. This is a convenience
// method: many callers will prefer to read from the channel C
// directly.
func (s *Subscription) Read() (*Message, error) {
	if !s.Active() {
		return nil, ErrCompletedSubscription
	}
	msg, ok := <-s.C
	if !ok {
		return nil, ErrCompletedSubscription
	}
	if msg.Err != nil {
		return nil, msg.Err
	}
	return msg, nil
}

func (s *Subscription) closeChannel(msg *Message) {
	if msg != nil {
		s.C <- msg
	}
	atomic.StoreInt32(&s.state, subStateClosed)
	close(s.C)
	close(s.closeChan)
}

func (s *Subscription) readLoop(ch chan *frame.Frame) {
	for {
		f, ok := <-ch
		if !ok {
			state := atomic.LoadInt32(&s.state)
			if state == subStateActive || state == subStateClosing {
				msg := &Message{
					Err: &Error{
						Message: fmt.Sprintf("Subscription %s: %s: channel read failed", s.id, s.destination),
					},
				}
				s.closeChannel(msg)
			}
			return
		}

		switch f.Command {
		case frame.MESSAGE:
			s.handleMessage(f)
		case frame.ERROR:
			s.handleError(f)
			return
		case frame.RECEIPT:
			s.handleReceipt(f)
			return
		default:
			log.Printf("Subscription %s: %s: unsupported frame type: %+v\n", s.id, s.destination, f)
		}

	}
}

func (s *Subscription) handleMessage(f *frame.Frame) {
	msg := &Message{
		Destination:  f.Header.Get(frame.Destination),
		ContentType:  f.Header.Get(frame.ContentType),
		Conn:         s.conn,
		Subscription: s,
		Header:       f.Header,
		Body:         f.Body,
	}
	s.C <- msg
}

func (s *Subscription) handleError(f *frame.Frame) {
	state := atomic.LoadInt32(&s.state)
	if state == subStateActive || state == subStateClosing {
		message, _ := f.Header.Contains(frame.Message)
		text := fmt.Sprintf("Subscription %s: %s: ERROR message:%s",
			s.id,
			s.destination,
			message)
		log.Println(text)
		contentType := f.Header.Get(frame.ContentType)
		msg := &Message{
			Err: &Error{
				Message: f.Header.Get(frame.Message),
				Frame:   f,
			},
			ContentType:  contentType,
			Conn:         s.conn,
			Subscription: s,
			Header:       f.Header,
			Body:         f.Body,
		}
		s.closeChannel(msg)
	}
}

func (s *Subscription) handleReceipt(f *frame.Frame) {
	state := atomic.LoadInt32(&s.state)
	if state == subStateActive || state == subStateClosing {
		s.closeChannel(nil)
	}
}
