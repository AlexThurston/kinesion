package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
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
		fmt.Println(err)
		return
	}

	for getStreamStatus(streamName) != kinesis.StreamStatusActive {
		fmt.Println("Waiting for stream to be created")
		time.Sleep(5 * time.Second)
	}
}

func listAllStreams() {
	listResp, err := k.ListStreams(ls)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(listResp)
}

func describeStream() {
	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}

	resp, err := k.DescribeStream(params)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(resp)
}

func getStreamStatus(streamName string) string {
	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}

	resp, err := k.DescribeStream(params)
	if err != nil {
		fmt.Println(err)
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
		fmt.Println(err)
		return
	}

	describeStream := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}
	for {
		descResp, err := k.DescribeStream(describeStream)
		if err != nil {
			fmt.Println("Stream has been removed")
			break
		}
		if *descResp.StreamDescription.StreamStatus == kinesis.StreamStatusDeleting {
			fmt.Println("Still deleting stream")
		}
		time.Sleep(5 * time.Second)
	}

	listResp, err := k.ListStreams(ls)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(listResp)
}

func startProducer() {
	message := "ALEX"
	for produce {
		message += "1"
		params := &kinesis.PutRecordInput{
			Data:         []byte(message),
			PartitionKey: aws.String("foo"),
			StreamName:   aws.String(streamName),
		}

		_, err := k.PutRecord(params)
		if err != nil {
			fmt.Println(err)
			return
		}
		// fmt.Printf("Produced message\n")
		time.Sleep(1 * time.Second)
	}
}

func startConsumer() {
	siParams := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String("0"),
		ShardIteratorType: aws.String(kinesis.ShardIteratorTypeLatest),
		StreamName:        aws.String(streamName),
	}

	siResp, err := k.GetShardIterator(siParams)
	if err != nil {
		fmt.Println(err)
		return
	}

	params := &kinesis.GetRecordsInput{
		ShardIterator: aws.String(*siResp.ShardIterator),
		Limit:         aws.Int64(1),
	}
	getResp, err := k.GetRecords(params)
	for _, record := range getResp.Records {
		fmt.Println(string(record.Data))
	}

	for consume {
		params := &kinesis.GetRecordsInput{
			ShardIterator: aws.String(*getResp.NextShardIterator),
			Limit:         aws.Int64(1),
		}

		getResp, err = k.GetRecords(params)
		if err != nil {
			fmt.Println(err)
			return
		}

		for _, record := range getResp.Records {
			fmt.Println(string(record.Data))
		}
		time.Sleep(1 * time.Second)
	}
}

func main() {
	fmt.Println("Hello Kinesis")

	fmt.Println("Creating stream...")
	createStream()

	fmt.Println("Listing all streams...")
	listAllStreams()

	fmt.Println("Describing created stream")
	describeStream()

	fmt.Println("Starting producer...")
	go startProducer()
	fmt.Println("Starting consumer...")
	go startConsumer()

	fmt.Println("Waiting 10 seconds...")
	time.Sleep(10 * time.Second)

	fmt.Println("Stopping producer and consumer...")
	produce = false
	consume = false

	fmt.Println("Deleting stream...")
	deleteStream()
}
