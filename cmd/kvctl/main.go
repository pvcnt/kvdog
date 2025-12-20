package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pvcnt/kvdog/internal/cli"
)

var (
	serverAddr string
)

var rootCmd = &cobra.Command{
	Use:          "kvctl",
	Short:        "kvctl is a CLI tool for interacting with the kvdog server",
	Long:         "kvctl is a command-line interface for managing key-value pairs on a kvdog gRPC server.",
	SilenceUsage: true,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&serverAddr, "server", "s", "localhost:50051", "gRPC server address")

	rootCmd.AddCommand(cli.NewGetCmd(&serverAddr))
	rootCmd.AddCommand(cli.NewPutCmd(&serverAddr))
	rootCmd.AddCommand(cli.NewDeleteCmd(&serverAddr))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
