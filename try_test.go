package rabbitmq

import (
	"testing"

	"errors"
	"fmt"
)

func TestDoWithPanic(t *testing.T) {
	SomeFunction := func() (string, error) {
		panic("something went badly wrong")
	}

	err := Do(func(attempt int) (retry bool, err error) {
		retry = attempt < 2 // try 2 times
		defer func() {
			if r := recover(); r != nil {
				err = errors.New(fmt.Sprintf("panic: %v", r))
			}
		}()
		_,err = SomeFunction()
		return
	})
	if err == nil {
		t.Errorf("Test Fail : Actual [%v]\tExptected [%v]\n", nil, "A Panic Occured")
	}
}

func TestDoWithErrorValue(t *testing.T) {
	SomeFunction := func() (string, error) {
		return "", errors.New("an error  occured ")
	}

	err := Do(func(attempt int) (retry bool, err error) {
		retry = attempt < 2 // try 2 times
		_,err = SomeFunction()
		return
	})
	if err == nil {
		t.Errorf("Test Fail : Actual [%v]\tExptected [%v]\n", nil, "Return An Error Value")
	}
}

func TestDoRetryLimit(t *testing.T) {
	counter := 0
	SomeFunction := func() (string, error) {
		return "", errors.New("an error  occured ")
	}

	Do(func(attempt int) (retry bool, err error) {
		retry = attempt < 2 // try 2 times
		counter++
		_,err = SomeFunction()
		return
	})

	if counter != 3 {
		t.Errorf("Test Fail : Actual [%v]\tExptected [%d]\n", "Retry Count is 2", counter)
	}
}
