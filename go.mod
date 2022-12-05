module github.com/Azure/azure-event-hubs-go/v3

go 1.18

require (
	github.com/Azure/azure-amqp-common-go/v3 v3.2.3
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-sdk-for-go v65.0.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/Azure/go-amqp v0.17.6-0.20221130234719-dd5083052614
	github.com/Azure/go-autorest/autorest v0.11.28
	github.com/Azure/go-autorest/autorest/adal v0.9.21
	github.com/Azure/go-autorest/autorest/azure/auth v0.4.2
	github.com/Azure/go-autorest/autorest/date v0.3.0
	github.com/Azure/go-autorest/autorest/to v0.4.0
	github.com/devigned/tab v0.1.1
	github.com/joho/godotenv v1.3.0
	github.com/jpillora/backoff v1.0.0
	github.com/mitchellh/mapstructure v1.5.0
	github.com/sirupsen/logrus v1.2.0
	github.com/stretchr/testify v1.7.1
	golang.org/x/net v0.0.0-20220725212005-46097bf591d3
)

require (
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/azure/cli v0.3.1 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dimchansky/utfbom v1.1.0 // indirect
	github.com/golang-jwt/jwt/v4 v4.4.2 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.1 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa // indirect
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/Azure/azure-amqp-common-go/v3 => github.com/jhendrixMSFT/azure-amqp-common-go/v3 v3.0.0-20221205224024-6831b15ba556
