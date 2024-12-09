package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	entity "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
)

type NotificationPayload struct {
	Operation string        `json:"operation"`
	ID        string        `json:"id"`
	Entity    entity.Entity `json:"data"`
}

type NotificationHandler func(np NotificationPayload)

// Subscribe subscribes to a specified PostgreSQL channel to receive notifications
// and ensures the trigger and notify function are created in the database.
//
// Parameters:
//   - channel: The name of the PostgreSQL channel to listen for notifications.
//   - handler: A callback function of type `NotificationHandler` that processes
//     the notification payload.
//
// Return Value:
//   - error: Returns an error if there is a failure in setting up the trigger,
//     subscribing to the channel, or receiving notifications.
func (dbs *PostgresDatabase) Subscribe(ef entity.EntityFactory, handler NotificationHandler) error {

	ctx := context.Background()

	// Ensure the trigger function and trigger are created
	err := dbs.ensureNotifyTrigger(ctx, ef)
	if err != nil {
		return fmt.Errorf("failed to ensure trigger setup: %w", err)
	}

	// Acquire a connection for listening
	conn, err := dbs.poolDb.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}

	channel := fmt.Sprintf("%s_changes", ef().TABLE())

	// Start listening to the channel
	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", channel))
	if err != nil {
		conn.Release()
		return fmt.Errorf("failed to listen to channel %s: %w", channel, err)
	}

	// Handle notifications on a separate goroutine
	go func() {
		defer conn.Release()
		entityFactory := ef
		for {
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				logger.Error("Error receiving notification from DB: %v", err)
				time.Sleep(time.Second) // Retry after a brief delay
				continue
			}

			var payload NotificationPayload
			payload.Entity = entityFactory()
			if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
				logger.Error("Error unmarshaling DB notification payload: %v", err)
				continue
			}

			// Process the notification
			handler(payload)
		}
	}()
	return nil
}

// ensureNotifyTrigger ensures that the `notify_stream_changes` function and trigger exist in the database.
func (dbs *PostgresDatabase) ensureNotifyTrigger(ctx context.Context, ef entity.EntityFactory) error {
	ddlTemplate := `
	CREATE OR REPLACE FUNCTION public.notify_%s_changes()
	RETURNS trigger
	LANGUAGE plpgsql
	AS $function$
	BEGIN
		-- Construct the payload
		PERFORM pg_notify(
			'%s_changes',
			json_build_object(
				'operation', TG_OP,
				'id', NEW.id,
				'data', CASE TG_OP
					WHEN 'DELETE' THEN OLD.data
					ELSE NEW.data
				END
			)::text
		);
		RETURN NEW;
	END;
	$function$;

	DO $$
	BEGIN
		IF NOT EXISTS (
			SELECT 1 FROM pg_trigger
			WHERE tgname = '%s_changes_trigger'
		) THEN
			CREATE TRIGGER %s_changes_trigger
			AFTER INSERT OR UPDATE OR DELETE ON %s
			FOR EACH ROW EXECUTE FUNCTION public.notify_%s_changes();
		END IF;
	END;
	$$;
	`
	tableName := ef().TABLE()
	ddl := fmt.Sprintf(ddlTemplate, tableName, tableName, tableName, tableName, tableName, tableName)
	_, err := dbs.poolDb.Exec(ctx, ddl)

	return err
}
