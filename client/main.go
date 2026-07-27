package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	pb "chat_grpc/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Ask for username
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter your username: ")
	username, err := reader.ReadString('\n')
	if err != nil {
		log.Fatalf("failed to read username: %v", err)
	}
	username = strings.TrimSpace(username)
	if username == "" {
		log.Fatalf("username cannot be empty")
	}

	// Connect to gRPC server
	serverAddr := "localhost:50051"
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewChatServiceClient(conn)
	stream, err := client.StreamChat(context.Background())
	if err != nil {
		log.Fatalf("failed to open stream: %v", err)
	}

	// Send initial message to register username
	err = stream.Send(&pb.ChatMessage{
		User:      username,
		Message:   "",
		Timestamp: time.Now().Unix(),
	})
	if err != nil {
		log.Fatalf("failed to register username: %v", err)
	}

	// Start a goroutine to receive messages from the server
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("\nDisconnected from server.")
				os.Exit(0)
			}
			if err != nil {
				fmt.Printf("\nError receiving: %v\n", err)
				os.Exit(1)
			}

			t := time.Unix(msg.Timestamp, 0).Format("15:04:05")
			if msg.User == "System" {
				fmt.Printf("\r[%s] %s\n> ", t, msg.Message)
			} else {
				fmt.Printf("\r[%s] %s: %s\n> ", t, msg.User, msg.Message)
			}
		}
	}()

	// Read input from user and send to server
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			fmt.Print("> ")
			continue
		}
		if text == "/quit" || text == "/exit" {
			break
		}

		err = stream.Send(&pb.ChatMessage{
			User:      username,
			Message:   text,
			Timestamp: time.Now().Unix(),
		})
		if err != nil {
			fmt.Printf("Error sending message: %v\n", err)
			break
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}
