package tenant_test

import "os"

func osWritePolicy(path, body string) error {
	return os.WriteFile(path, []byte(body), 0o600)
}
