// Package web is upkeeper's way to the web: search through a SearXNG, read
// pages as plain text, clean what leaves the lab out of queries, and check that
// a quote is really on a page.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"golang.org/x/net/html"
)

// Client searches and reads.
type Client struct {
	// SearXNG is the search's base URL.
	SearXNG string
	// Allow are the domains whose pages are read from search results.
	Allow []string
	HTTP  *http.Client
}

// New returns a client.
func New(searxng string, allow []string) *Client {
	return &Client{SearXNG: strings.TrimRight(searxng, "/"), Allow: allow, HTTP: &http.Client{Timeout: 15 * time.Second}}
}

// Result is a search hit.
type Result struct {
	Title, URL, Snippet string
}

// ErrNothing is a search with no usable results, e.g. every engine refusing.
var ErrNothing = errors.New("the search found nothing on the allowed sites")

// Search returns up to n results on allowed domains.
func (c *Client) Search(ctx context.Context, query string, n int) ([]Result, error) {
	u := c.SearXNG + "/search?" + url.Values{"q": {query}, "format": {"json"}}.Encode()
	var res struct {
		Results []struct {
			Title   string `json:"title"`
			URL     string `json:"url"`
			Content string `json:"content"`
		} `json:"results"`
		Unresponsive [][]string `json:"unresponsive_engines"`
	}
	if err := c.get(ctx, u, 4<<20, func(r io.Reader) error { return json.NewDecoder(r).Decode(&res) }); err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	var out []Result
	seen := map[string]bool{}
	for _, r := range res.Results {
		if !c.Allowed(r.URL) || seen[r.URL] {
			continue
		}
		seen[r.URL] = true
		out = append(out, Result{strings.TrimSpace(r.Title), r.URL, strings.TrimSpace(r.Content)})
		if len(out) == n {
			break
		}
	}
	if len(out) == 0 {
		var down []string
		for _, e := range res.Unresponsive {
			down = append(down, strings.Join(e, ": "))
		}
		if len(down) > 0 {
			return nil, fmt.Errorf("%w (engines down: %s)", ErrNothing, strings.Join(down, ", "))
		}
		return nil, ErrNothing
	}
	return out, nil
}

// Allowed reports whether u is https or http on an allowed domain.
func (c *Client) Allowed(u string) bool {
	p, err := url.Parse(u)
	if err != nil || p.Scheme != "https" && p.Scheme != "http" {
		return false
	}
	host := strings.ToLower(p.Hostname())
	return slices.ContainsFunc(c.Allow, func(d string) bool {
		d = strings.ToLower(d)
		return host == d || strings.HasSuffix(host, "."+d)
	})
}

// Page is a page as text.
type Page struct {
	Title, Text string
	// Gone is a page that no longer exists.
	Gone bool
}

// maxText bounds a page's text; the useful part of docs is near the top.
const maxText = 60_000

// Read fetches u and returns its text. Any page may be read; callers decide
// which.
func (c *Client) Read(ctx context.Context, u string) (Page, error) {
	var p Page
	err := c.get(ctx, u, 2<<20, func(r io.Reader) error {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if strings.Contains(strings.ToLower(string(b[:min(len(b), 512)])), "<html") || strings.Contains(strings.ToLower(string(b[:min(len(b), 512)])), "<!doctype") {
			p.Title, p.Text = text(string(b))
		} else {
			p.Text = string(b)
		}
		return nil
	})
	var se statusError
	if errors.As(err, &se) && (se == http.StatusNotFound || se == http.StatusGone) {
		return Page{Gone: true}, nil
	}
	if len(p.Text) > maxText {
		p.Text = p.Text[:maxText]
	}
	return p, err
}

type statusError int

func (e statusError) Error() string { return fmt.Sprintf("HTTP %d", int(e)) }

func (c *Client) get(ctx context.Context, u string, limit int64, read func(io.Reader) error) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "upkeeper (homelab agent)")
	req.Header.Set("Accept", "text/html,text/plain,application/json;q=0.9,*/*;q=0.5")
	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return statusError(res.StatusCode)
	}
	return read(io.LimitReader(res.Body, limit))
}

// skip are elements that aren't a page's content.
var skip = map[string]bool{"script": true, "style": true, "noscript": true, "nav": true, "header": true, "footer": true, "aside": true, "svg": true, "form": true, "button": true, "template": true}

var block = map[string]bool{"p": true, "div": true, "li": true, "tr": true, "br": true, "h1": true, "h2": true, "h3": true, "h4": true, "pre": true, "section": true, "article": true, "blockquote": true, "dd": true, "dt": true}

// text is an HTML page's title and readable text: its main or article
// element if it has one, else its body.
func text(doc string) (string, string) {
	root, err := html.Parse(strings.NewReader(doc))
	if err != nil {
		return "", ""
	}
	var title string
	var main, body *html.Node
	var find func(*html.Node)
	find = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "title":
				if title == "" && n.FirstChild != nil {
					title = strings.TrimSpace(n.FirstChild.Data)
				}
			case "main", "article":
				if main == nil {
					main = n
				}
			case "body":
				body = n
			}
		}
		for ch := n.FirstChild; ch != nil; ch = ch.NextSibling {
			find(ch)
		}
	}
	find(root)
	from := main
	if from == nil {
		from = body
	}
	if from == nil {
		from = root
	}
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && skip[n.Data] {
			return
		}
		if n.Type == html.TextNode {
			// Line breaks in HTML source are spaces; only block elements break lines.
			b.WriteString(strings.ReplaceAll(n.Data, "\n", " "))
		}
		for ch := n.FirstChild; ch != nil; ch = ch.NextSibling {
			walk(ch)
		}
		if n.Type == html.ElementNode && block[n.Data] {
			b.WriteString("\n")
		}
	}
	walk(from)
	var lines []string
	for l := range strings.SplitSeq(b.String(), "\n") {
		if l = strings.Join(strings.Fields(l), " "); l != "" {
			lines = append(lines, l)
		}
	}
	return title, strings.Join(lines, "\n")
}

var (
	reIPv4  = regexp.MustCompile(`\b\d{1,3}(\.\d{1,3}){3}(:\d+)?\b`)
	reIPv6  = regexp.MustCompile(`(?i)\b[0-9a-f]{1,4}(:[0-9a-f]{0,4}){3,7}\b`)
	reUUID  = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	reHex   = regexp.MustCompile(`(?i)\b[0-9a-f]{12,}\b`)
	rePod   = regexp.MustCompile(`\b[a-z0-9]([a-z0-9-]*[a-z0-9])?-[a-z0-9]{8,10}-[a-z0-9]{5}\b`)
	reEmail = regexp.MustCompile(`\b[\w.+-]+@[\w-]+(\.[\w-]+)+\b`)
)

// Clean removes from a query what could identify the lab: addresses, ids, pod
// names, and every word of private (hostnames, project names, domains), so
// what's left is error text and public product names.
func Clean(query string, private []string) string {
	s := query
	for _, re := range []*regexp.Regexp{reEmail, reUUID, reIPv4, reIPv6, rePod, reHex} {
		s = re.ReplaceAllString(s, " ")
	}
	for _, w := range private {
		if w = strings.TrimSpace(w); w == "" {
			continue
		}
		// A private word removes the whole token it's in, e.g. a hostname
		// under a private domain or a namespace named after a project.
		re := regexp.MustCompile(`(?i)[^\s"'()]*` + regexp.QuoteMeta(w) + `[^\s"'()]*`)
		s = re.ReplaceAllString(s, " ")
	}
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 200 {
		s = s[:200]
	}
	return strings.Trim(s, " -:/")
}

// words are a text's lower-case words of three letters or more.
func words(s string) []string {
	var out []string
	for f := range strings.FieldsFuncSeq(strings.ToLower(s), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' && r != '-' && r != '.'
	}) {
		if f = strings.Trim(f, ".-"); len(f) >= 3 {
			out = append(out, f)
		}
	}
	return out
}

// Contains reports whether quote is on the page word for word, ignoring case,
// spacing and punctuation: how a model's quote is checked before it's used.
func Contains(page, quote string) bool {
	q := words(quote)
	if len(q) < 3 {
		return false
	}
	return strings.Contains(" "+strings.Join(words(page), " ")+" ", " "+strings.Join(q, " ")+" ")
}

// Still reports how much of quote is still on the page: the best share of its
// words found within any stretch of the page twice its length. Docs get
// reflowed and reworded slightly; a changed answer scores low.
func Still(page, quote string) float64 {
	q := words(quote)
	p := words(page)
	if len(q) == 0 {
		return 0
	}
	want := map[string]int{}
	for _, w := range q {
		want[w]++
	}
	size := 2 * len(q)
	have := map[string]int{}
	matched, best := 0, 0
	for i, w := range p {
		if have[w] < want[w] {
			matched++
		}
		have[w]++
		if i >= size {
			old := p[i-size]
			have[old]--
			if have[old] < want[old] {
				matched--
			}
		}
		best = max(best, matched)
	}
	return float64(best) / float64(len(q))
}
