package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
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

// Subscribe subscribes to entity change notifications with auto-reconnect & keepalive.
//
// Resilience features:
//   - Dedicated pooled connection held for LISTEN
//   - Exponential backoff with jitter on errors (incl. "conn closed")
//   - Periodic keepalive (SELECT 1) to avoid idle reaping
//   - Context-aware shutdown
func (dbs *PostgresDatabase) Subscribe(ef entity.EntityFactory, handler NotificationHandler) error {

	ctx := context.Background()

	// One-time DDL guard
	if err := dbs.ensureNotifyTrigger(ctx, ef); err != nil {
		return fmt.Errorf("failed to ensure trigger setup: %w", err)
	}

	channel := fmt.Sprintf("%s_changes", ef().TABLE())
	if !isSafeChannel(channel) {
		return fmt.Errorf("unsafe channel name: %q", channel)
	}

	// Tunables
	const (
		keepaliveEvery = 2 * time.Minute // ping cadence during idle
		waitTimeout    = 2 * time.Minute // how long WaitForNotification blocks before we ping
		backoffBase    = 250 * time.Millisecond
		backoffMax     = 15 * time.Second
		maxBackoffExp  = 6 // base<<6 ~= ~16s cap before clamping to backoffMax
	)

	// Reconnect loop
	go func() {
		backoffAttempt := 0

		for {
			// Respect caller cancellation
			if ctx.Err() != nil {
				return
			}

			// Acquire a dedicated connection from the pool
			pconn, err := dbs.poolDb.Acquire(ctx)
			if err != nil {
				logger.Error("DB LISTEN acquire failed: %v", err)
				sleepCtx(ctx, jittered(backoffBase, backoffAttempt, int(backoffMax), maxBackoffExp))
				backoffAttempt++
				continue
			}

			conn := pconn.Conn()
			acquiredAt := time.Now()

			// LISTEN (quote identifier safely)
			channel := fmt.Sprintf("%s_changes", ef().TABLE())
			listenSQL := "LISTEN " + channel
			if _, err := conn.Exec(ctx, listenSQL); err != nil {
				logger.Error("DB LISTEN exec failed on %s: %v", channel, err)
				pconn.Release()
				sleepCtx(ctx, jittered(backoffBase, backoffAttempt, int(backoffMax), maxBackoffExp))
				backoffAttempt++
				continue
			}

			logger.Info("DB LISTEN established on channel=%s", channel)

			// Inner receive loop; reset backoff after a successful (re)subscribe
			backoffAttempt = 0
			lastPing := time.Now()

			for {
				// If caller canceled, cleanly unlisten and release
				if ctx.Err() != nil {
					// Best effort UNLISTEN; ignore errors
					_, _ = conn.Exec(context.Background(), "UNLISTEN *")
					pconn.Release()
					return
				}

				// Wait with a timeout so we can ping periodically
				waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
				notification, err := conn.WaitForNotification(waitCtx)
				cancel()

				switch {
				case err == nil && notification != nil:
					var payload NotificationPayload
					payload.Entity = ef()
					if uerr := json.Unmarshal([]byte(notification.Payload), &payload); uerr != nil {
						logger.Error("DB notify unmarshal error: %v", uerr)
						continue
					}
					// Process user handler (protect against panics)
					func() {
						defer func() {
							if r := recover(); r != nil {
								logger.Error("panic in notification handler: %v", r)
							}
						}()
						handler(payload)
					}()

				case err == context.DeadlineExceeded:
					// Idle too long; send a keepalive ping to avoid idle disconnects
					if time.Since(lastPing) >= keepaliveEvery {
						if _, pingErr := conn.Exec(ctx, "SELECT 1"); pingErr != nil {
							logger.Warn("DB LISTEN keepalive failed (will reconnect): %v", pingErr)
							// Break to reconnect logic below
							err = pingErr
						} else {
							lastPing = time.Now()
						}
					}
					// Continue inner loop unless ping failed
					if err == context.DeadlineExceeded {
						continue
					}
					// else err was set to pingErr, fallthrough to reconnect

				default:
					// Any error from WaitForNotification (incl. "conn closed") triggers reconnect
				}

				if err != nil {
					// Release and back off before trying to re-subscribe
					pconn.Release()
					logger.Warn("DB LISTEN loop error on %s (held %.0fs): %v",
						channel, time.Since(acquiredAt).Seconds(), err)
					sleepCtx(ctx, jittered(backoffBase, backoffAttempt, int(backoffMax), maxBackoffExp))
					backoffAttempt++
					break // break inner loop → outer reconnect
				}
			}
		}
	}()

	return nil
}

// isSafeChannel ensures we only interpolate simple identifiers in LISTEN.
func isSafeChannel(s string) bool {
	for _, r := range s {
		if !(r == '_' || r == '-' || (r >= '0' && r <= '9') ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			return false
		}
	}
	return len(s) > 0
}

func jittered(base time.Duration, attempt, maxExp int, maxCap time.Duration) time.Duration {
	shift := attempt
	if shift > maxExp {
		shift = maxExp
	}
	d := base << shift
	if d > maxCap {
		d = maxCap
	}
	// add 0–250ms jitter
	j := time.Duration(rand.Intn(250)) * time.Millisecond
	return d + j
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
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
