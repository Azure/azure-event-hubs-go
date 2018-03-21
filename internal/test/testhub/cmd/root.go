package cmd

import (
	"fmt"
	"os"

	"github.com/Azure/azure-amqp-common-go/conn"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.PersistentFlags().StringVar(&namespace, "namespace", "", "namespace of the Event Hub")
	rootCmd.PersistentFlags().StringVar(&hubName, "hub", "", "name of the Event Hub")
	rootCmd.PersistentFlags().StringVar(&sasKeyName, "key-name", "", "SAS key name for the Event Hub")
	rootCmd.PersistentFlags().StringVar(&sasKey, "key", "", "SAS key for the key-name")
	rootCmd.PersistentFlags().StringVar(&connStr, "conn-str", "", "Connection string for Event Hub")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "debug level logging")
}

var (
	namespace, suffix, hubName, sasKeyName, sasKey, connStr string
	debug                                                   bool

	rootCmd = &cobra.Command{
		Use:              "hubtest",
		Short:            "hubtest is a simple command line testing tool for the Event Hub library",
		TraverseChildren: true,
	}
)

// Execute kicks off the command line
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func checkAuthFlags() error {
	if connStr != "" {
		parsed, err := conn.ParsedConnectionFromStr(connStr)
		if err != nil {
			return err
		}
		namespace = parsed.Namespace
		hubName = parsed.HubName
		suffix = parsed.Suffix
		sasKeyName = parsed.KeyName
		sasKey = parsed.Key
		return nil
	}

	if namespace == "" {
		return errors.New("namespace is required")
	}

	if hubName == "" {
		return errors.New("hubName is required")
	}

	if sasKey == "" {
		return errors.New("key is required")
	}

	if sasKeyName == "" {
		return errors.New("key-name is required")
	}
	return nil
}

func environment() azure.Environment {
	env := azure.PublicCloud
	if suffix != "" {
		env.ServiceBusEndpointSuffix = suffix
	}
	return env
}
