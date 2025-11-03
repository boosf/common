package messagebroker

type Message struct {
	PartitionKey string
	Body         string
}
