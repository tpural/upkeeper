package config

import (
	"strings"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	t.Setenv("TREMOLO_TOKEN", "tok")
	c, err := Parse([]byte("tremolo: {url: http://tremolo}\nmodel: {url: http://m/v1, name: q}\npatrol: {at: \"07:30\", timezone: America/Chicago}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Tremolo.Token != "tok" || c.Name != "upkeeper" || c.Every.Duration != 30*time.Second || c.GPU.WakeDelay.Duration != 3*time.Minute || c.GPU.MaxWakesPerDay != 6 || c.Model.MaxSteps != 30 {
		t.Errorf("defaults: %+v", c)
	}
	_, err = Parse([]byte("model: {url: x}\ngpu: {deployment: nope}\npatrol: {at: '7pm'}\n"))
	for _, want := range []string{"tremolo.url", "model.url and model.name", "namespace/name", "HH:MM"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error %v lacks %q", err, want)
		}
	}
	if _, err := Parse([]byte("tremolo: {url: x}\nmodel: {url: x, name: y}\ntypo: 1\n")); err == nil {
		t.Error("unknown key accepted")
	}
}
