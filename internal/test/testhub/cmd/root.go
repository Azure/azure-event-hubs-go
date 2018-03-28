package cmd

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"fmt"
	"os"

	"github.com/Azure/azure-amqp-common-go/conn"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.PersistentFlags().StringVar(&namespace, "namespace", "", "namespace of the Event Hub")
	rootCmd.PersistentFlags().StringVar(&hubName, "hub", "", "name of the Event Hub")
	rootCmd.PersistentFlags().StringVar(&sasKeyName, "key-name", "", "SAS key name for the Event Hub")
	rootCmd.PersistentFlags().StringVar(&sasKey, "key", "", "SAS key for the key-name")
	rootCmd.PersistentFlags().StringVar(&connStr, "conn-str", "", "Connection string for Event Hub")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "debug level logging")
	log.SetFormatter(&log.TextFormatter{TimestampFormat: "2006-01-02 15:04:05", FullTimestamp: true})
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
