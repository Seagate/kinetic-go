package kinetic

import (
	"testing"
  "math/rand"
  "time"
  "strconv"
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

func TestGet(t *testing.T) {
  client, _ := Connect("localhost:8123")
  defer client.Close()

  rand.Seed(time.Now().UnixNano())
  specKey := "Key_" + strconv.FormatInt(rand.Int63(), 10)
  specVal := "Val_" + strconv.FormatInt(rand.Int63(), 10)

  rx0, _ := client.Put([]byte(specKey), []byte(specVal))
  
  if err := <-rx0; err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  vChan, sChan, _ := client.Get([]byte(specKey))
  select {
  case v := <- vChan:
    if string(v) != specVal {
      t.Errorf("got %v\nwant %v", string(v), string(specVal))
    }
  case s := <- sChan:
    t.Errorf("got %v\nwant success", s)
  }
}
