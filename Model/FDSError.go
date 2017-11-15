package Model

import (
	"fmt"
	"runtime"
	"time"
)

type FDSError struct {
	code     int
	time     time.Time
	msg      string
	funcName string
}

func (e *FDSError) Error() string {
	return fmt.Sprintf("%s %s Code: [%d] Msg: ", e.time.Format(time.ANSIC), e.funcName, e.code, e.msg)
}

func (e *FDSError) Code() int {
	return e.code
}

func (e *FDSError) Message() string {
	return e.msg
}

func NewFDSError(msg string, code int) *FDSError {

	pc, _, _, _ := runtime.Caller(1)

	return &FDSError{
		code:     code,
		msg:      msg,
		time:     time.Now(),
		funcName: runtime.FuncForPC(pc).Name(),
	}
}
