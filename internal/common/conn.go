package common

import (
	"errors"
	"fmt"
	"regexp"
)

var (
	connStrRegex = regexp.MustCompile(`Endpoint=sb:\/\/(?P<Host>.+?);SharedAccessKeyName=(?P<KeyName>.+?);SharedAccessKey=(?P<Key>.+?);EntityPath=(?P<HubName>.+)`)
	hostStrRegex = regexp.MustCompile(`^(?P<Namespace>.+?)\.(.+?)\/`)
)

type (
	// ParsedConn is the structure of a parsed Service Bus or Event Hub connection string.
	ParsedConn struct {
		Host      string
		Suffix    string
		Namespace string
		HubName   string
		KeyName   string
		Key       string
	}
)

// newParsedConnection is a constructor for a parsedConn and verifies each of the inputs is non-null.
func newParsedConnection(host, suffix, namespace, hubName, keyName, key string) (*ParsedConn, error) {
	if host == "" || keyName == "" || key == "" {
		return nil, errors.New("connection string contains an empty entry")
	}
	return &ParsedConn{
		Host:      "amqps://" + host,
		Suffix:    suffix,
		Namespace: namespace,
		KeyName:   keyName,
		Key:       key,
		HubName:   hubName,
	}, nil
}

// ParsedConnectionFromStr takes a string connection string from the Azure portal and returns the parsed representation.
func ParsedConnectionFromStr(connStr string) (*ParsedConn, error) {
	matches := connStrRegex.FindStringSubmatch(connStr)
	namespaceMatches := hostStrRegex.FindStringSubmatch(matches[1])
	fmt.Println(matches[1], namespaceMatches[2], namespaceMatches[1], matches[2], matches[3])
	return newParsedConnection(matches[1], namespaceMatches[2], namespaceMatches[1], matches[4], matches[2], matches[3])
}
