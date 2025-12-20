package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/pvcnt/kvdog/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "Path to configuration file")
	metadataPath := flag.String("metadata", "", "Path to sharding metadata file")
	flag.Parse()

	if *configPath == "" {
		return fmt.Errorf("configuration file is required")
	}
	if *metadataPath == "" {
		return fmt.Errorf("metadata file is required")
	}

	// Load configuration
	cfg, err := server.LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	// Load sharding metadata
	metadata, err := server.LoadMetadata(*metadataPath)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %v", err)
	}

	log.Printf("Loaded sharding metadata with %d shards", len(metadata.Shards))

	s, err := server.NewServer(cfg, metadata)
	if err != nil {
		return fmt.Errorf("failed to create server: %v", err)
	}

	// Set up gRPC server
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", cfg.GRPCAddr, err)
	}

	// Start gRPC server in a goroutine
	go func() {
		log.Printf("gRPC server listening on %s", cfg.GRPCAddr)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	// Graceful shutdown
	fmt.Println("\nShutting down...")
	if err := s.Shutdown(); err != nil {
		return fmt.Errorf("failed to gracefully shutdown server: %v", err)
	}
	return nil
}
