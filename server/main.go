package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	pb "chat_grpc/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedChatServiceServer
	mu      sync.Mutex
	clients map[string]pb.ChatService_StreamChatServer
}

func newServer() *server {
	return &server{
		clients: make(map[string]pb.ChatService_StreamChatServer),
	}
}

// StreamChat handles bidirectional streaming of chat messages.
func (s *server) StreamChat(stream pb.ChatService_StreamChatServer) error {
	var username string

	// Loop to receive messages from the client
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error receiving from client: %v", err)
			break
		}

		s.mu.Lock()
		// Register user on their first message
		if username == "" {
			username = msg.User
			// Check if username is already taken
			if _, exists := s.clients[username]; exists {
				s.mu.Unlock()
				errMsg := &pb.ChatMessage{
					User:      "System",
					Message:   fmt.Sprintf("Username '%s' is already taken. Connection closing.", username),
					Timestamp: time.Now().Unix(),
				}
				_ = stream.Send(errMsg)
				return status.Errorf(codes.AlreadyExists, "username already taken")
			}
			s.clients[username] = stream
			log.Printf("User %s connected.", username)
			s.mu.Unlock()

			// Broadcast join message
			s.broadcast(&pb.ChatMessage{
				User:      "System",
				Message:   fmt.Sprintf("%s joined the chat", username),
				Timestamp: time.Now().Unix(),
			})
			continue
		}
		s.mu.Unlock()

		// Broadcast standard user message
		log.Printf("[%s]: %s", msg.User, msg.Message)
		s.broadcast(msg)
	}

	// Handle cleanup on disconnection
	if username != "" {
		s.mu.Lock()
		delete(s.clients, username)
		log.Printf("User %s disconnected.", username)
		s.mu.Unlock()

		s.broadcast(&pb.ChatMessage{
			User:      "System",
			Message:   fmt.Sprintf("%s left the chat", username),
			Timestamp: time.Now().Unix(),
		})
	}

	return nil
}

// broadcast sends a message to all active clients.
func (s *server) broadcast(msg *pb.ChatMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for user, stream := range s.clients {
		err := stream.Send(msg)
		if err != nil {
			log.Printf("Failed to send message to user %s: %v", user, err)
		}
	}
}

func main() {
	port := ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChatServiceServer(grpcServer, newServer())

	log.Printf("gRPC Chat Server is running on port %s...", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
