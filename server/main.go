package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	pb "chat_grpc/proto"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	mongoClient *mongo.Client
	msgCol      *mongo.Collection
)

// initMongo connects to the MongoDB instance and pings it to ensure connectivity.
func initMongo() {
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}

	log.Printf("Connecting to MongoDB at %s...", uri)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("failed to ping MongoDB: %v", err)
	}

	mongoClient = client
	msgCol = client.Database("chat_db").Collection("messages")
	log.Println("Successfully connected to MongoDB!")
}

// saveMessage inserts a message into MongoDB.
func saveMessage(msg *pb.ChatMessage) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := msgCol.InsertOne(ctx, bson.M{
		"user":      msg.User,
		"message":   msg.Message,
		"timestamp": msg.Timestamp,
	})
	if err != nil {
		log.Printf("Error saving message to MongoDB: %v", err)
	}
}

// sendHistory retrieves the last 50 messages from MongoDB and sends them to the stream.
func sendHistory(stream pb.ChatService_StreamChatServer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := options.Find().
		SetLimit(50).
		SetSort(bson.M{"timestamp": -1})

	cursor, err := msgCol.Find(ctx, bson.D{}, opts)
	if err != nil {
		return fmt.Errorf("failed to retrieve history: %w", err)
	}
	defer cursor.Close(ctx)

	var dbMsgs []struct {
		User      string `bson:"user"`
		Message   string `bson:"message"`
		Timestamp int64  `bson:"timestamp"`
	}

	if err := cursor.All(ctx, &dbMsgs); err != nil {
		return fmt.Errorf("failed to decode history: %w", err)
	}

	// Send messages in chronological order (oldest first)
	for i := len(dbMsgs) - 1; i >= 0; i-- {
		chatMsg := &pb.ChatMessage{
			User:      dbMsgs[i].User,
			Message:   dbMsgs[i].Message,
			Timestamp: dbMsgs[i].Timestamp,
		}
		if err := stream.Send(chatMsg); err != nil {
			return err
		}
	}

	return nil
}

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
		if username == "" {
			username = msg.User
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

			// Send message history to the newly connected user first
			if err := sendHistory(stream); err != nil {
				log.Printf("Failed to send history to %s: %v", username, err)
			}

			// Broadcast join message
			s.broadcast(&pb.ChatMessage{
				User:      "System",
				Message:   fmt.Sprintf("%s joined the chat", username),
				Timestamp: time.Now().Unix(),
			})
			continue
		}
		s.mu.Unlock()

		// Save message to MongoDB (do not persist empty/system messages)
		if msg.User != "System" && msg.Message != "" {
			saveMessage(msg)
		}

		// Broadcast standard user message
		log.Printf("[%s]: %s", msg.User, msg.Message)
		s.broadcast(msg)
	}

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
	// Initialize MongoDB connection
	initMongo()
	defer func() {
		if mongoClient != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := mongoClient.Disconnect(ctx); err != nil {
				log.Printf("Error disconnecting from MongoDB: %v", err)
			}
		}
	}()

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
