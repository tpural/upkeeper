package tremolo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestAwait(t *testing.T) {
	awaitEvery = time.Millisecond
	var mu sync.Mutex
	kind, claims, denied := Awaiting, 0, false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/investigations/3/claim":
			claims++
			if claims == 3 {
				kind = "action"
			}
			fmt.Fprintf(w, `{"id":3,"events":[{"id":1,"kind":"note"},{"id":7,"kind":%q,"text":"approved by tp"}]}`, kind)
		case "/api/investigations/3/actions/7/deny":
			denied = true
			_, _ = w.Write([]byte(`{"id":7,"kind":"denied","text":"withdrawn by upkeeper"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()
	c, _ := New(ts.URL, "tok", "upkeeper")
	ctx := context.Background()

	// Approved on the third look, with the claim kept meanwhile.
	e, err := c.Await(ctx, 3, 7, time.Minute)
	if err != nil || e.Kind != "action" || claims != 3 {
		t.Errorf("approved = %+v, %v after %d claims", e, err, claims)
	}
	// Nobody decides in time: it's withdrawn.
	kind, claims = Awaiting, -100
	e, err = c.Await(ctx, 3, 7, 0)
	if err != nil || e.Kind != "expired" || !denied {
		t.Errorf("late = %+v, %v, withdrawn %v", e, err, denied)
	}
}
