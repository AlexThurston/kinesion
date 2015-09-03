package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"log"
	"time"
)

var config = aws.NewConfig().WithRegion("us-east-1")
var k = kinesis.New(config)
var err error
var streamName string = "test-stream"
var ls = &kinesis.ListStreamsInput{}
var produce = true
var consume = true

func createStream() {
	cs := &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int64(1),
	}

	_, err := k.CreateStream(cs)
	if err != nil {
		log.Println(err)
		return
	}

	for getStreamStatus(streamName) != kinesis.StreamStatusActive {
		log.Println("Waiting for stream to be created")
		time.Sleep(5 * time.Second)
	}
}

func listAllStreams() {
	listResp, err := k.ListStreams(ls)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(listResp)
}

func describeStream() {
	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}

	resp, err := k.DescribeStream(params)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(resp)
}

func getStreamStatus(streamName string) string {
	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}

	resp, err := k.DescribeStream(params)
	if err != nil {
		log.Println(err)
		return ""
	}

	return *resp.StreamDescription.StreamStatus
}

func deleteStream() {
	ds := &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	}
	_, err = k.DeleteStream(ds)
	if err != nil {
		log.Println(err)
		return
	}

	describeStream := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}
	for {
		descResp, err := k.DescribeStream(describeStream)
		if err != nil {
			log.Println("Stream has been removed")
			break
		}
		if *descResp.StreamDescription.StreamStatus == kinesis.StreamStatusDeleting {
			log.Println("Still deleting stream")
		}
		time.Sleep(5 * time.Second)
	}

	listResp, err := k.ListStreams(ls)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(listResp)
}

func startProducer(done chan bool) {
	message := "ALEX"
	for {
		select {
		case <-done:
			return
		default:
			message += "1"
			params := &kinesis.PutRecordInput{
				Data:         []byte(message),
				PartitionKey: aws.String("foo"),
				StreamName:   aws.String(streamName),
			}

			_, err := k.PutRecord(params)
			if err != nil {
				log.Println(err)
				return
			}
			// log.Printf("Produced message\n")
			time.Sleep(1 * time.Second)
		}
	}
}

func startConsumer(done chan bool) {
	siParams := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String("0"),
		ShardIteratorType: aws.String(kinesis.ShardIteratorTypeLatest),
		StreamName:        aws.String(streamName),
	}

	siResp, err := k.GetShardIterator(siParams)
	if err != nil {
		log.Println(err)
		return
	}

	params := &kinesis.GetRecordsInput{
		ShardIterator: aws.String(*siResp.ShardIterator),
		Limit:         aws.Int64(1),
	}
	getResp, err := k.GetRecords(params)
	for _, record := range getResp.Records {
		log.Println(string(record.Data))
	}

	for {
		select {
		case <-done:
			return
		default:
			params := &kinesis.GetRecordsInput{
				ShardIterator: aws.String(*getResp.NextShardIterator),
				Limit:         aws.Int64(1),
			}

			getResp, err = k.GetRecords(params)
			if err != nil {
				log.Println(err)
				return
			}

			for _, record := range getResp.Records {
				log.Println(string(record.Data))
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func main() {
	consumerDone := make(chan bool)
	producerDone := make(chan bool)

	log.Println("Hello Kinesis")

	log.Println("Creating stream...")
	createStream()

	log.Println("Listing all streams...")
	listAllStreams()

	log.Println("Describing created stream")
	describeStream()

	log.Println("Starting producer...")
	go startProducer(producerDone)
	log.Println("Starting consumer...")
	go startConsumer(consumerDone)

	log.Println("Waiting 10 seconds...")
	time.Sleep(10 * time.Second)

	log.Println("Stopping producer and consumer...")
	producerDone <- true
	consumerDone <- true

	log.Println("Deleting stream...")
	deleteStream()
}
