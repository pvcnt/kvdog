package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/pvcnt/kvdog/internal/client"
	pb "github.com/pvcnt/kvdog/pkg/proto"
)

func NewDeleteCmd(serverAddr *string) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]

			c, err := client.New(*serverAddr)
			if err != nil {
				return err
			}
			defer c.Close()

			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()

			_, err = c.DeleteItem(ctx, &pb.DeleteItemRequest{Key: key})
			if err != nil {
				return fmt.Errorf("failed to delete key: %w", err)
			}

			fmt.Printf("Successfully deleted %q\n", key)
			return nil
		},
	}
}
