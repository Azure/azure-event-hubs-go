package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	namespace = "mynamespace"
	keyName   = "keyName"
	secret    = "superSecret"
	connStr   = "Endpoint=sb://" + namespace + ".servicebus.windows.net/;SharedAccessKeyName=" + keyName + ";SharedAccessKey=" + secret
)

func TestParsedConnectionFromStr(t *testing.T) {
	parsed, err := ParsedConnectionFromStr(connStr)
	assert.Nil(t, err, err)
	assert.Equal(t, "amqps://"+namespace+".servicebus.windows.net/", parsed.Host)
	assert.Equal(t, namespace, parsed.Namespace)
	assert.Equal(t, keyName, parsed.KeyName)
	assert.Equal(t, secret, parsed.Key)
}
