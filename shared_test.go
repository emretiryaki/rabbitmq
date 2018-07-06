package rabbitmq

import (
	"testing"

	"github.com/google/uuid"
)

func TestIsGuid(t *testing.T) {
	if isGuid("test") {
		t.Logf("Test Success : Actual [%d]\tExptected [%d]\n", false, false)
	}
	if isGuid(uuid.New().String()) {
		t.Logf("Test Success : Actual [%d]\tExptected [%d]\n", true, true)
	}
}

func TestGetGuid(t *testing.T) {
	var guid = getGuid()
	_, err := uuid.Parse(guid)
	if err != nil {
		t.Errorf("Test Failed : Actual [%d]\tExptected [%d]\n", guid, err.Error())
	}
}
