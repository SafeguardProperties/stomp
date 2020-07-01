package stomp

import (
	"github.com/go-stomp/stomp/frame"
)

// Error values
var (
	ErrInvalidCommand        = newErrorMessage("invalid command")
	ErrInvalidFrameFormat    = newErrorMessage("invalid frame format")
	ErrUnsupportedVersion    = newErrorMessage("unsupported version")
	ErrCompletedTransaction  = newErrorMessage("transaction is completed")
	ErrNackNotSupported      = newErrorMessage("NACK not supported in STOMP 1.0")
	ErrNotReceivedMessage    = newErrorMessage("cannot ack/nack a message, not from server")
	ErrCannotNackAutoSub     = newErrorMessage("cannot send NACK for a subscription with ack:auto")
	ErrCompletedSubscription = newErrorMessage("subscription is unsubscribed")
	ErrClosedUnexpectedly    = newErrorMessage("connection closed unexpectedly")
	ErrAlreadyClosed         = newErrorMessage("connection already closed")
	ErrMsgSendTimeout        = newErrorMessage("msg send timeout")
	ErrNilOption             = newErrorMessage("nil option")
	ErrReadTimeout           = newErrorMessage("read timeout")
	ErrConnectionClosed      = newErrorMessage("connection closed")
	ErrMissingMessageId      = newErrorMessage("missing header: " + frame.MessageId)
	ErrMissingAck            = newErrorMessage("missing header: " + frame.Ack)
)

// StompError implements the Error interface, and provides
// additional information about a STOMP error.
type Error struct {
	Message string
	Frame   *frame.Frame
}

func (e Error) Error() string {
	return e.Message
}

func newErrorMessage(msg string) Error {
	return Error{Message: msg}
}

func newError(f *frame.Frame) Error {
	e := Error{Frame: f}

	if f.Command == frame.ERROR {
		if message := f.Header.Get(frame.Message); message != "" {
			e.Message = message
		} else {
			e.Message = "ERROR frame, missing message header"
		}
	} else {
		e.Message = "Unexpected frame: " + f.Command
	}
	return e
}
