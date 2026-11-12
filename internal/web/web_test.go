package web

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClean(t *testing.T) {
	private := []string{"tapslab", "gridtie", "pi-80438b1b", "mm02"}
	for in, want := range map[string]string{
		"gridtie-7664dbbb58-5cjpq on pi-80438b1b: exec /app/server: exec format error":              "on exec /app/server: exec format error",
		"dial tcp 10.106.190.173:4318: connect: no route to host":                                   "dial tcp : connect: no route to host",
		"tremolo.lan.tapslab.com x509: certificate signed by unknown authority":                     "x509: certificate signed by unknown authority",
		"helmrelease upgrade retries exhausted uid 65b9dc91-867d-4d7e-a676-3ff0ed69c6b2 on mm02":    "helmrelease upgrade retries exhausted uid on",
		"containerd 26421d672ded19f30e22f7b8ce105797967dcc5f2f88b325906e6156ddf182f6 overlay error": "containerd overlay error",
		"mail admin@example.com about flux kustomization failing":                                   "mail about flux kustomization failing",
	} {
		if got := Clean(in, private); got != want {
			t.Errorf("Clean(%q) = %q, want %q", in, got, want)
		}
	}
}

const page = `<!doctype html><html><head><title>Multi-platform images | Docs</title><script>var x="exec format error"</script></head>
<body><nav>Home Docs Blog</nav><main><h1>Multi-platform images</h1>
<p>If a container exits with <code>exec format error</code>, the image was built for a different CPU
architecture than the node it runs on.</p><p>Build for both with <b>docker buildx --platform linux/amd64,linux/arm64</b>,
or schedule the pod on a matching node with a nodeSelector on kubernetes.io/arch.</p></main>
<footer>Copyright</footer></body></html>`

func TestReadAndQuotes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/doc":
			_, _ = w.Write([]byte(page))
		case "/search":
			_, _ = w.Write([]byte(`{"results":[{"title":"Spam","url":"https://spam.example/x","content":"x"},
				{"title":"Multi-platform","url":"https://docs.docker.com/build/multi-platform/","content":"exec format error"},
				{"title":"Dup","url":"https://docs.docker.com/build/multi-platform/","content":""}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := New(srv.URL, []string{"docker.com"})
	ctx := context.Background()

	p, err := c.Read(ctx, srv.URL+"/doc")
	if err != nil || p.Title != "Multi-platform images | Docs" || strings.Contains(p.Text, "Home Docs") || strings.Contains(p.Text, "var x") ||
		!strings.Contains(p.Text, "the image was built for a different CPU architecture") {
		t.Fatalf("read %+v, %v", p, err)
	}
	if gone, _ := c.Read(ctx, srv.URL+"/moved"); !gone.Gone {
		t.Error("a 404 isn't gone")
	}

	// A quote must be on the page word for word; spacing and case don't matter.
	if !Contains(p.Text, "If a container exits with exec format error, the image was built for a different CPU architecture") {
		t.Error("real quote rejected")
	}
	if Contains(p.Text, "If a container exits with exec format error, reinstall the kernel") {
		t.Error("made-up quote accepted")
	}

	// Rechecks: the same answer reflowed scores high, a changed one low.
	quote := "schedule the pod on a matching node with a nodeSelector on kubernetes.io/arch"
	if s := Still("Docs v2.\nOr: schedule the pod on a\nmatching node, with a nodeSelector\non kubernetes.io/arch.", quote); s < 0.8 {
		t.Errorf("reflowed scores %.2f", s)
	}
	if s := Still("Use runtimeClassName to run foreign binaries through qemu emulation instead.", quote); s >= 0.8 {
		t.Errorf("changed scores %.2f", s)
	}

	res, err := c.Search(ctx, "exec format error", 3)
	if err != nil || len(res) != 1 || res[0].URL != "https://docs.docker.com/build/multi-platform/" {
		t.Errorf("search %+v, %v", res, err)
	}
	c.Allow = []string{"kubernetes.io"}
	if _, err := c.Search(ctx, "x", 3); !errors.Is(err, ErrNothing) {
		t.Errorf("no allowed results: %v", err)
	}
}
