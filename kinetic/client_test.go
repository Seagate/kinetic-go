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

func TestGetVersion(t *testing.T) {
  client, _ := Connect("localhost:8123")
  defer client.Close()

  rand.Seed(time.Now().UnixNano())
  specKey := "Key_" + strconv.FormatInt(rand.Int63(), 10)
  specVal := "Val_" + strconv.FormatInt(rand.Int63(), 10)

  rx0, _ := client.Put([]byte(specKey), []byte(specVal))
  
  if err := <-rx0; err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  vChan, sChan, _ := client.GetVersion([]byte(specKey))
  select {
  case v := <- vChan:
    if string(v) != "" {
      t.Errorf("got %v\nwant %v", string(v), "")
    }
  case s := <- sChan:
    t.Errorf("got %v\nwant success", s)
  }
}

func TestGetNextKey(t *testing.T) {
  client, _ := Connect("localhost:8123")
  defer client.Close()

  specKey0 := "Key_0"
  specVal0 := "Val_0"

  rx0, _ := client.Put([]byte(specKey0), []byte(specVal0))
  
  if err := <-rx0; err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  specKey1 := "Key_1"
  specVal1 := "Val_1"

  rx1, _ := client.Put([]byte(specKey1), []byte(specVal1))
  
  if err := <-rx1; err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  vChan, sChan, _ := client.GetNextKey([]byte(specKey0))
  select {
  case v := <- vChan:
    if string(v) != specKey1 {
      t.Errorf("got %v\nwant %v", string(v), specKey1)
    }
  case s := <- sChan:
    t.Errorf("got %v\nwant success", s)
  }
}

func TestGetPreviousKey(t *testing.T) {
  client, _ := Connect("localhost:8123")
  defer client.Close()

  specKey0 := "Key_0"
  specVal0 := "Val_0"

  rx0, _ := client.Put([]byte(specKey0), []byte(specVal0))
  
  if err := <-rx0; err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  specKey1 := "Key_1"
  specVal1 := "Val_1"

  rx1, _ := client.Put([]byte(specKey1), []byte(specVal1))
  
  if err := <-rx1; err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  vChan, sChan, _ := client.GetPreviousKey([]byte(specKey1))
  select {
  case v := <- vChan:
    if string(v) != specKey0 {
      t.Errorf("got %v\nwant %v", string(v), specKey0)
    }
  case s := <- sChan:
    t.Errorf("got %v\nwant success", s)
  }
}

func TestGetKeyRange(t *testing.T) {
  client, _ := Connect("localhost:8123")
  defer client.Close()

  specKey0 := "Key_0"
  specVal0 := "Val_0"

  rx0, _ := client.Put([]byte(specKey0), []byte(specVal0))
  
  if err := <-rx0; err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  specKey1 := "Key_1"
  specVal1 := "Val_1"

  rx1, _ := client.Put([]byte(specKey1), []byte(specVal1))
  
  if err := <-rx1; err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  vChan, sChan, _ := client.GetKeyRange([]byte(specKey0), []byte(specKey1))
  select {
  case v := <- vChan:
    if len(v) != 2 {
      t.Errorf("got %v\nwant %v", len(v), 2)
    }
  case s := <- sChan:
    t.Errorf("got %v\nwant success", s)
  }
}
