package clickhouse

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/elk-utilities/prism/bench/internal/caps"
)

// WriteBenchConfig emits ClickHouse config.d/users.d snippets capped to budget.
func WriteBenchConfig(dir string, budget caps.Budget) error {
	if err := os.MkdirAll(filepath.Join(dir, "config.d"), 0o750); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dir, "users.d"), 0o750); err != nil {
		return err
	}
	serverMem := budget.ClickHouseServerMaxMemoryBytes()
	configXML := fmt.Sprintf(`<clickhouse>
    <max_server_memory_usage>%d</max_server_memory_usage>
</clickhouse>
`, serverMem)
	usersXML := fmt.Sprintf(`<clickhouse>
    <profiles>
        <default>
            <max_threads>%d</max_threads>
            <max_memory_usage>%d</max_memory_usage>
        </default>
    </profiles>
</clickhouse>
`, budget.Threads(), budget.ClickHouseMaxMemoryBytes())
	if err := os.WriteFile(filepath.Join(dir, "config.d", "bench-caps.xml"), []byte(configXML), 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "users.d", "bench-caps.xml"), []byte(usersXML), 0o600)
}
