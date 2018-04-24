package rabbitmq

import (
	"testing"

	"fmt"
	"errors"
)

func TestDoWithPanic(t *testing.T) {
	SomeFunction := func() (string, error) {
		panic("something went badly wrong")
	}

	var value string
	err := Do(func(attempt int) (retry bool, err error) {
		retry = attempt < 2 // try 2 times
		defer func(){
			if r := recover(); r != nil {
				err = errors.New(fmt.Sprintf("panic: %v", r))
			}
		}()
		value,err=SomeFunction()
		return
	})
	if err == nil {
		t.Errorf("Test Fail : Actual [%d]\tExptected [%d]\n", nil,"A Panic Occured")
	}
 }

func TestDoWithErrorValue(t *testing.T) {
	SomeFunction := func() (string, error) {
		 return "",errors.New("an error  occured ")
	}

	var value string
	err := Do(func(attempt int) (retry bool, err error) {
		retry = attempt < 2 // try 2 times
		value,err=SomeFunction()
		return
	})
	if err == nil {
		t.Errorf("Test Fail : Actual [%d]\tExptected [%d]\n",nil, "Return An Error Value")
	}
}


func TestDoRetryLimit(t *testing.T) {
	counter :=0
	SomeFunction := func() (string, error) {
		return "",errors.New("an error  occured ")
	}

	var value string
	Do(func(attempt int) (retry bool, err error) {
		retry = attempt < 2 // try 2 times
		counter++
		value,err=SomeFunction()
		return
	})

	if counter != 3 {
		t.Errorf("Test Fail : Actual [%d]\tExptected [%d]\n" ,"Retry Count is 2",counter)
	}
}




