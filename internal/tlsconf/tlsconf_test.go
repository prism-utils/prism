package tlsconf

import "testing"

func TestValidate(t *testing.T) {
	if err := (*Config)(nil).Validate("t"); err != nil {
		t.Fatalf("nil config should validate: %v", err)
	}
	if err := (&Config{}).Validate("t"); err != nil {
		t.Fatalf("empty config should validate: %v", err)
	}
	if err := (&Config{Cert: "c.pem"}).Validate("t"); err == nil {
		t.Fatal("cert without key should fail")
	}
	if err := (&Config{Key: "k.pem"}).Validate("t"); err == nil {
		t.Fatal("key without cert should fail")
	}
	if err := (&Config{Cert: "c.pem", Key: "k.pem"}).Validate("t"); err != nil {
		t.Fatalf("paired cert/key should validate: %v", err)
	}
}

func TestBuild(t *testing.T) {
	if cfg, err := (*Config)(nil).Build(); err != nil || cfg != nil {
		t.Fatalf("nil config: cfg=%v err=%v", cfg, err)
	}
	cfg, err := (&Config{ServerName: "host", InsecureSkipVerify: true}).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if cfg.ServerName != "host" || !cfg.InsecureSkipVerify {
		t.Fatalf("tls config not applied: %+v", cfg)
	}
	if _, err := (&Config{CA: "/no/such/ca.pem"}).Build(); err == nil {
		t.Fatal("missing CA file should error")
	}
}
