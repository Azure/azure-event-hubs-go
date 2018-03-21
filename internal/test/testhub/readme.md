## Hub Testing CLI
`testhub` is a command line tool to help test Event Hub sending and receiving.

### Usage
When simply executing `testhub`, you will be greeted with a help page. `help` can be called on each command.
```bash
$ testhub
hubtest is a simple command line testing tool for the Event Hub library

Usage:
  hubtest [command]

Available Commands:
  delete      Delete an Event Hub
  help        Help about any command
  receive     Receive messages from an Event Hub
  send        Send messages to an Event Hub
  version     Print the git ref

Flags:
      --conn-str string    Connection string for Event Hub
      --debug              debug level logging
  -h, --help               help for hubtest
      --hub string         name of the Event Hub
      --key string         SAS key for the key-name
      --key-name string    SAS key name for the Event Hub
      --namespace string   namespace of the Event Hub

Use "hubtest [command] --help" for more information about a command.
```

#### Send
Send will by default send 1 messages of 256 bytes.
```bash
$ testhub send --namespace yourNamespace --hub yourHub --key-name yourKeyName --key yourKey
```
Send with a connection string.
```bash
$ testhub send --conn-str yourConnectionString
```

You can specify more messages by using `--msg-count` and `--msg-size`.
```bash
$ testhub send --conn-str yourConnectionString --msg-count 100 --msg-size 100
```

#### Receive
Listen for messages on all partitions of a given Event Hub.
```bash
$ testhub receive --namespace yourNamespace --hub yourHub --key-name yourKeyName --key yourKey
```
Use a connection string instead.
```bash
$ testhub receive --conn-str yourConnectionString
```

#### Debug
If you would like to see more in-depth information about what is happening, run the commands with `--debug`.