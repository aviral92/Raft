package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	//	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-aviral92/pb"
)

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no endpoint fail
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	endpoint := flag.Args()[0]
	log.Printf("Connecting to %v", endpoint)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")
	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)
	// Clear KVC
	//go func(){
	res, err := kvc.Clear(context.Background(), &pb.Empty{})
	redirect := res.GetRedirect()
	if redirect != nil {
		log.Printf("Redirect to server %v", redirect)
	} else {
		if err != nil {
			log.Fatalf("Could not clear")
		}
	}
	//}()

	// Put setting hello -> 1
	//go func() {
	putReq := &pb.KeyValue{Key: "hello", Value: "1"}
	res, err = kvc.Set(context.Background(), putReq)
	redirect = res.GetRedirect()
	if redirect != nil {
		log.Printf("Redirect to server %v", redirect)
	} else {
		if err != nil {
			log.Fatalf("Put error")
		}
		log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
			log.Fatalf("Put returned the wrong response")
		}
	}
	//}()

	// Request value for hello
	//go func() {
	req := &pb.Key{Key: "hello"}
	res, err = kvc.Get(context.Background(), req)
	redirect = res.GetRedirect()
	if redirect != nil {
		log.Printf("Redirect to server %v", redirect)
	} else {

		if err != nil {
			log.Fatalf("Request error %v", err)
		}
		log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
			log.Fatalf("Get returned the wrong response")
		}
	}
	//}()

	// Successfully CAS changing hello -> 2
	//go func() {
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	redirect = res.GetRedirect()
	if redirect != nil {
		log.Printf("Redirect to server %v", redirect)
	} else {

		if err != nil {
			log.Fatalf("Request error %v", err)
		}
		log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		if res.GetKv().Key != "hello" || res.GetKv().Value != "2" {
			log.Fatalf("Get returned the wrong response")
		}
	}
	//}()

	// Unsuccessfully CAS
	//go func(){
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}
	res, err = kvc.CAS(context.Background(), casReq)
	redirect = res.GetRedirect()
	if redirect != nil {
		log.Printf("Redirect to server %v", redirect)
	} else {

		if err != nil {
			log.Fatalf("Request error %v", err)
		}
		log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		if res.GetKv().Key != "hello" || res.GetKv().Value == "3" {
			log.Fatalf("Get returned the wrong response")
		}
	}
	//}()

	// CAS should fail for uninitialized variables
	//go func(){
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	redirect = res.GetRedirect()
	if redirect != nil {
		log.Printf("Redirect to server %v", redirect)
	} else {

		if err != nil {
			log.Fatalf("Request error %v", err)
		}
		log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
			log.Fatalf("Get returned the wrong response")
		}
	}
	//}()
	//time.Sleep(100000* time.Millisecond)
}
