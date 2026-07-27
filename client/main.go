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

	// Connect to gRPC-Web / standard gRPC server on port 8080
	serverAddr := "localhost:8080"
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewChatServiceClient(conn)

	// Call JoinChat (Server-Streaming RPC) to connect and start receiving messages
	stream, err := client.JoinChat(context.Background(), &pb.JoinRequest{User: username})
	if err != nil {
		log.Fatalf("failed to join chat: %v", err)
	}

	// Start a goroutine to receive messages from the server stream
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

	// Read input from user and send via SendMessage (Unary RPC)
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

		// Send message using SendMessage (Unary call)
		_, err = client.SendMessage(context.Background(), &pb.ChatMessage{
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
