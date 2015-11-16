package kinetic

import (
	"testing"
)

// go test ./kinetic

func TestPut(t *testing.T) {
	client, _ := Connect("localhost:8123")
	defer client.Close()

	rxs, _ := client.Put([]byte("Key"), []byte("Value"))

	if err := <-rxs; err != nil {
		t.Errorf("got %v\nwant success", err)
	}
}

func TestDelete(t *testing.T) {
  client, _ := Connect("localhost:8123")
  defer client.Close()

  rxs, _ := client.Delete([]byte("Key"))

  if err := <-rxs; err != nil {
    t.Errorf("got %v\nwant success", err)
  }
}
