package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	pb "chat_grpc/proto"

	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
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
func sendHistory(stream pb.ChatService_JoinChatServer) error {
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
	clients map[string]pb.ChatService_JoinChatServer
}

func newServer() *server {
	return &server{
		clients: make(map[string]pb.ChatService_JoinChatServer),
	}
}

// JoinChat registers the user, plays history, and streams incoming messages to them.
func (s *server) JoinChat(req *pb.JoinRequest, stream pb.ChatService_JoinChatServer) error {
	username := req.User
	if username == "" {
		return status.Errorf(codes.InvalidArgument, "username cannot be empty")
	}

	s.mu.Lock()
	// Check if username is already taken
	if _, exists := s.clients[username]; exists {
		s.mu.Unlock()
		return status.Errorf(codes.AlreadyExists, "username '%s' already taken", username)
	}
	s.clients[username] = stream
	log.Printf("User %s connected.", username)
	s.mu.Unlock()

	// 1. Send message history to the newly connected user
	if err := sendHistory(stream); err != nil {
		log.Printf("Failed to send history to %s: %v", username, err)
	}

	// 2. Broadcast join message to everyone
	s.broadcast(&pb.ChatMessage{
		User:      "System",
		Message:   fmt.Sprintf("%s joined the chat", username),
		Timestamp: time.Now().Unix(),
	})

	// 3. Block until the client stream closes (connection disconnects)
	<-stream.Context().Done()

	// 4. Handle cleanup on disconnect
	s.mu.Lock()
	delete(s.clients, username)
	log.Printf("User %s disconnected.", username)
	s.mu.Unlock()

	s.broadcast(&pb.ChatMessage{
		User:      "System",
		Message:   fmt.Sprintf("%s left the chat", username),
		Timestamp: time.Now().Unix(),
	})

	return nil
}

// SendMessage handles unary messages from clients, saves them to MongoDB, and broadcasts them.
func (s *server) SendMessage(ctx context.Context, msg *pb.ChatMessage) (*pb.Empty, error) {
	if msg.User == "" {
		return nil, status.Errorf(codes.InvalidArgument, "sender user cannot be empty")
	}

	// Save message to MongoDB (do not persist empty/system messages)
	if msg.User != "System" && msg.Message != "" {
		saveMessage(msg)
	}

	// Broadcast user message
	log.Printf("[%s]: %s", msg.User, msg.Message)
	s.broadcast(msg)

	return &pb.Empty{}, nil
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

	grpcServer := grpc.NewServer()
	pb.RegisterChatServiceServer(grpcServer, newServer())

	// Wrap gRPC server with gRPC-Web compatibility wrapper
	wrappedServer := grpcweb.WrapServer(
		grpcServer,
		grpcweb.WithOriginFunc(func(origin string) bool {
			return true // Allow CORS from any origin
		}),
	)

	// Combine gRPC-Web and standard CORS preflights
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, x-user-agent, x-grpc-web, grpc-timeout")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Route both standard gRPC and gRPC-Web requests to the gRPC server
		if wrappedServer.IsGrpcWebRequest(r) || r.Header.Get("Content-Type") == "application/grpc" {
			wrappedServer.ServeHTTP(w, r)
			return
		}

		// Simple standard HTTP status response
		w.Write([]byte("gRPC-Web Chat Backend is running! Connect using a gRPC-Web client."))
	})

	port := ":8080"
	log.Printf("gRPC-Web Chat Server is running on port %s...", port)
	h2s := &http2.Server{}
	if err := http.ListenAndServe(port, h2c.NewHandler(httpHandler, h2s)); err != nil {
		log.Fatalf("failed to serve HTTP: %v", err)
	}
}
