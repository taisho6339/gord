package chord

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/pkg/model"
	"testing"
)

func BenchmarkFingerTableStabilize(b *testing.B) {
	log.SetLevel(log.FatalLevel)
	processes := generateStabilizedProcesses(context.Background(), 100)
	findingKey := model.NewHashID("test")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := processes[0].FindSuccessorByTable(context.Background(), findingKey)
		if err != nil {
			b.Fatalf("err = %#v", err)
		}
	}
}
