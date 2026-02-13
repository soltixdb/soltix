# Logging Package

Package logging provides a structured logging wrapper for Zerolog with context-aware features, HTTP middleware, and configuration management.

## Features

- ✅ Structured logging with Zerolog (zero allocation, fastest performance)
- ✅ Context-aware logging with request ID, user ID, trace ID
- ✅ HTTP middleware for Fiber framework
- ✅ Auto-generate request ID
- ✅ Development and Production modes
- ✅ JSON and Console output formats
- ✅ Global and instance loggers
- ✅ Configurable log levels and output paths
- 🚀 Zero allocation for maximum performance

## Installation

```bash
go get github.com/rs/zerolog
go get github.com/google/uuid
```

## Quick Start

### 1. Initialize Logger

```go
import "github.com/soltixdb/soltix/internal/logging"

func main() {
    // Development mode (console, colorized)
    logger := logging.NewDevelopment()
    defer logger.Sync()

    // Production mode (JSON format)
    logger := logging.NewProduction()
    defer logger.Sync()

    // Set as global logger
    logging.SetGlobal(logger)
}
```

### 2. Basic Logging

```go
// Simple logging
logging.Info("Server started")
logging.Error("Failed to connect")

// With fields
logging.Info("User logged in",
    logging.String("user_id", "123"),
    logging.Int("attempts", 3),
)

// With error
logging.Error("Database error",
    logging.Err(err),
    logging.String("query", sql),
)
```

### 3. HTTP Middleware (Fiber)

```go
import (
    "github.com/soltixdb/soltix/internal/logging"
    "github.com/gofiber/fiber/v2"
)

func main() {
    app := fiber.New()
    
    // Initialize logger
    logger := logging.NewProduction()
    defer logger.Sync()
    
    // Add middleware
    app.Use(logging.FiberMiddleware(logger))
    
    // Or with custom config
    cfg := logging.MiddlewareConfig{
        SkipPaths: []string{"/health", "/metrics"},
        AdditionalFields: func(c *fiber.Ctx) []logging.Field {
            return []logging.Field{
                logging.String("user_agent", c.Get("User-Agent")),
            }
        },
    }
    app.Use(logging.FiberMiddlewareWithConfig(logger, cfg))
    
    app.Listen(":8080")
}
```

### 4. Context-Aware Logging

```go
import "context"

func HandleRequest(ctx context.Context) {
    // Log with context (automatically includes request_id, user_id, trace_id)
    logging.InfoCtx(ctx, "Processing request",
        logging.String("action", "create_user"),
    )
    
    // Add custom context values
    ctx = logging.WithRequestID(ctx, "req-123")
    ctx = logging.WithUserID(ctx, "user-456")
    ctx = logging.WithTraceID(ctx, "trace-789")
    
    // Logs will automatically include IDs
    logging.InfoCtx(ctx, "Request completed")
    // Output: {"level":"info","timestamp":"...","msg":"Request completed","request_id":"req-123","user_id":"user-456","trace_id":"trace-789"}
}
```

### 5. Load from Configuration

```go
// Config struct
cfg := logging.Config{
    Level:      "debug",        // debug, info, warn, error
    Format:     "json",         // json, console
    OutputPath: "stdout",       // stdout, stderr, /path/to/file.log
}

logger, err := logging.NewFromConfig(cfg)
if err != nil {
    panic(err)
}
defer logger.Sync()

// Or use preset configs
cfg := logging.DefaultConfig()      // Production: info, json
cfg := logging.DevelopmentConfig()  // Development: debug, console
```

### 6. Structured Fields

Package provides type-safe field constructors:

```go
logging.Info("Event occurred",
    logging.String("name", "John"),           // String field
    logging.Int("age", 30),                   // Int field
    logging.Int64("timestamp", time.Now().Unix()), // Int64 field
    logging.Float64("price", 99.99),          // Float64 field
    logging.Bool("active", true),             // Bool field
    logging.Err(err),                         // Error field
    logging.Duration("elapsed", duration),    // Duration field
    logging.Any("metadata", map[string]string{"key": "val"}), // Any type
)

// Namespace for grouping fields
logging.Info("User data",
    logging.Namespace("user"),
    logging.String("id", "123"),
    logging.String("name", "John"),
)
// Output: {"level":"info","msg":"User data","user":{"id":"123","name":"John"}}
```

## API Reference

### Logger Methods

```go
logger.Debug(msg string, fields ...Field)  // Debug level
logger.Info(msg string, fields ...Field)   // Info level
logger.Warn(msg string, fields ...Field)   // Warning level
logger.Error(msg string, fields ...Field)  // Error level
logger.Fatal(msg string, fields ...Field)  // Fatal level (exits)
logger.Panic(msg string, fields ...Field)  // Panic level (panics)
logger.With(fields ...Field) *Logger       // Create child logger with fields
logger.WithContext(ctx context.Context) *Logger // Create logger with context fields
logger.Sync() error                        // Flush buffered logs
```

### Global Functions

```go
logging.Debug(msg, fields...)    // Use global logger
logging.Info(msg, fields...)
logging.Warn(msg, fields...)
logging.Error(msg, fields...)
logging.Fatal(msg, fields...)
logging.With(fields...)          // Create child from global
logging.Sync()                   // Flush global logger
```

### Context Functions

```go
logging.DebugCtx(ctx, msg, fields...)  // Log with context
logging.InfoCtx(ctx, msg, fields...)
logging.WarnCtx(ctx, msg, fields...)
logging.ErrorCtx(ctx, msg, fields...)
logging.FatalCtx(ctx, msg, fields...)

logging.WithRequestID(ctx, id)    // Add request ID to context
logging.WithUserID(ctx, id)       // Add user ID to context
logging.WithTraceID(ctx, id)      // Add trace ID to context
logging.FromContext(ctx)          // Get logger from context
```

## Examples

### Example 1: HTTP Handler with Context Logging

```go
func WriteHandler(c *fiber.Ctx) error {
    ctx := c.UserContext()
    
    // Log with automatic request_id from middleware
    logging.InfoCtx(ctx, "Received write request",
        logging.Int("points", len(points)),
    )
    
    if err := storage.Write(ctx, points); err != nil {
        logging.ErrorCtx(ctx, "Write failed",
            logging.Err(err),
            logging.Int("points", len(points)),
        )
        return c.Status(500).JSON(fiber.Map{"error": err.Error()})
    }
    
    logging.InfoCtx(ctx, "Write successful")
    return c.JSON(fiber.Map{"status": "ok"})
}
```

### Example 2: Child Logger with Persistent Fields

```go
// Create logger with persistent fields
storageLogger := logging.Global().With(
    logging.String("component", "storage"),
    logging.String("shard_id", "shard-1"),
)

// All logs will include component and shard_id
storageLogger.Info("Writing data")
storageLogger.Error("Write failed", logging.Err(err))
```

### Example 3: Custom Middleware Config

```go
cfg := logging.MiddlewareConfig{
    SkipPaths: []string{"/health", "/metrics", "/favicon.ico"},
    AdditionalFields: func(c *fiber.Ctx) []logging.Field {
        return []logging.Field{
            logging.String("user_agent", c.Get("User-Agent")),
            logging.String("referer", c.Get("Referer")),
            logging.Int("content_length", len(c.Body())),
        }
    },
}

app.Use(logging.FiberMiddlewareWithConfig(logger, cfg))
```

### Example 4: Load Config from YAML

```yaml
# config.yaml
logging:
  level: info
  format: json
  output_path: /var/log/soltix.log
```

```go
import "github.com/spf13/viper"

// Load config
viper.SetConfigFile("config.yaml")
viper.ReadInConfig()

cfg := logging.Config{
    Level:      viper.GetString("logging.level"),
    Format:     viper.GetString("logging.format"),
    OutputPath: viper.GetString("logging.output_path"),
}

logger, err := logging.NewFromConfig(cfg)
```

## Log Levels

| Level | Usage | Example |
|-------|-------|---------|
| `debug` | Development debugging | Variable values, function calls |
| `info` | Normal operations | Server started, request completed |
| `warn` | Warning conditions | Deprecated API used, high memory |
| `error` | Error conditions | Failed to connect, invalid input |
| `fatal` | Fatal errors (exits) | Cannot start server |

## Output Formats

### JSON Format (Production)
```json
{
  "level": "info",
  "timestamp": "2025-12-26T10:30:00.000Z",
  "msg": "Request completed",
  "method": "POST",
  "path": "/api/v1/write",
  "status": 200,
  "duration": "15ms",
  "request_id": "abc-123"
}
```

### Console Format (Development)
```
2025-12-26T10:30:00.000+0700    INFO    Request completed
    method=POST path=/api/v1/write status=200 duration=15ms request_id=abc-123
```

## Best Practices

### ✅ DO

```go
// Use structured fields
logging.Info("User created", logging.String("user_id", id))

// Use context logging in handlers
logging.InfoCtx(ctx, "Processing")

// Use child loggers for components
dbLogger := logger.With(logging.String("component", "database"))

// Sync before exit
defer logger.Sync()

// Use appropriate log levels
logging.Error("Critical error", logging.Err(err))  // For errors
logging.Info("Normal operation")                   // For info
```

### ❌ DON'T

```go
// Don't use string formatting
logging.Info(fmt.Sprintf("User %s created", id))  // ❌

// Don't ignore sync errors in production
logger.Sync()  // ❌ Should check error

// Don't log sensitive data
logging.Info("Password", logging.String("pass", password))  // ❌

// Don't use Fatal in libraries (only in main)
logger.Fatal("Error")  // ❌ (exits entire process)
```

## Performance Tips

1. **Use structured fields** instead of string formatting (faster)
2. **Reuse child loggers** with persistent fields
3. **Skip paths** in middleware to reduce overhead for health checks
4. **Flush periodically** with `logger.Sync()` to avoid losing logs on crash

## Testing

```go
// Use development logger in tests
func TestSomething(t *testing.T) {
    logger, _ := logging.NewDevelopment()
    defer logger.Sync()
    
    // Test code with logging
}

// Or create test logger
func TestWithLogger(t *testing.T) {
    logger := logging.NewTest(t)  // Logs to testing.T
    // Test code
}
```

## Thread Safety

All logger methods are **thread-safe** and can be called concurrently from multiple goroutines.

## Common Use Cases

### Use Case 1: Service Startup Logging

```go
func main() {
    logger, err := logging.NewProduction()
    if err != nil {
        log.Fatal(err)
    }
    defer logger.Sync()
    
    logging.SetGlobal(logger)
    
    logging.Info("Starting Soltix server",
        logging.String("version", version),
        logging.Int("port", port),
        logging.String("environment", env),
    )
    
    // Service initialization...
}
```

### Use Case 2: Database Operations

```go
func (s *Storage) Write(ctx context.Context, points []DataPoint) error {
    logger := logging.FromContext(ctx).With(
        logging.String("operation", "write"),
        logging.Int("points", len(points)),
    )
    
    logger.Debug("Starting write operation")
    
    if err := s.validate(points); err != nil {
        logger.Error("Validation failed", logging.Err(err))
        return err
    }
    
    if err := s.persist(points); err != nil {
        logger.Error("Persist failed", logging.Err(err))
        return err
    }
    
    logger.Info("Write completed successfully")
    return nil
}
```

### Use Case 3: Background Workers

```go
func processQueue(ctx context.Context) {
    logger := logging.Global().With(
        logging.String("worker", "queue-processor"),
        logging.String("queue_id", queueID),
    )
    
    logger.Info("Worker started")
    
    for {
        select {
        case <-ctx.Done():
            logger.Info("Worker stopping")
            return
        case msg := <-queue:
            logger.Debug("Processing message",
                logging.String("msg_id", msg.ID),
            )
            
            if err := process(msg); err != nil {
                logger.Error("Processing failed",
                    logging.Err(err),
                    logging.String("msg_id", msg.ID),
                )
                continue
            }
            
            logger.Debug("Message processed successfully",
                logging.String("msg_id", msg.ID),
            )
        }
    }
}
```

### Use Case 4: Error Recovery

```go
func handleWithRecovery(c *fiber.Ctx) (err error) {
    defer func() {
        if r := recover(); r != nil {
            logging.ErrorCtx(c.UserContext(), "Panic recovered",
                logging.Any("panic", r),
                logging.String("stack", string(debug.Stack())),
            )
            err = fiber.NewError(500, "Internal server error")
        }
    }()
    
    // Handler logic...
    return nil
}
```

## Logging Patterns

### Pattern 1: Request Scoped Logger

```go
func Middleware(logger *logging.Logger) fiber.Handler {
    return func(c *fiber.Ctx) error {
        requestLogger := logger.With(
            logging.String("request_id", c.Get("X-Request-ID")),
            logging.String("path", c.Path()),
        )
        
        ctx := logging.WithLogger(c.UserContext(), requestLogger)
        c.SetUserContext(ctx)
        
        return c.Next()
    }
}
```

### Pattern 2: Component Logger

```go
type Service struct {
    logger *logging.Logger
}

func NewService(name string) *Service {
    return &Service{
        logger: logging.Global().With(
            logging.String("service", name),
        ),
    }
}

func (s *Service) DoWork() {
    s.logger.Info("Work started")
    // ... work ...
    s.logger.Info("Work completed")
}
```

### Pattern 3: Conditional Logging

```go
func processLargeDataset(ctx context.Context, data []Item) {
    logger := logging.FromContext(ctx)
    
    logger.Info("Processing dataset", logging.Int("items", len(data)))
    
    for i, item := range data {
        if err := process(item); err != nil {
            logger.Error("Item processing failed",
                logging.Int("index", i),
                logging.String("item_id", item.ID),
                logging.Err(err),
            )
            continue
        }
        
        // Log progress every 1000 items
        if (i+1)%1000 == 0 {
            logger.Debug("Progress update",
                logging.Int("processed", i+1),
                logging.Int("total", len(data)),
            )
        }
    }
    
    logger.Info("Dataset processing completed")
}
```

## Configuration Examples

### Production Configuration

```go
cfg := logging.Config{
    Level:      "info",
    Format:     "json",
    OutputPath: "/var/log/soltix/app.log",
}
```

### Development Configuration

```go
cfg := logging.Config{
    Level:      "debug",
    Format:     "console",
    OutputPath: "stdout",
}
```

### High-Volume Production Configuration

```go
cfg := logging.Config{
    Level:      "warn",        // Only warnings and errors
    Format:     "json",
    OutputPath: "stdout",      // Let container orchestrator handle logs
}
```

## Troubleshooting

### Problem: Logs not appearing

**Solution**: Make sure to call `logger.Sync()` before program exits:
```go
defer logger.Sync()
```

### Problem: Too verbose in production

**Solution**: Set appropriate log level:
```go
cfg := logging.Config{
    Level: "info", // or "warn"
}
```

### Problem: Missing request_id in logs

**Solution**: Ensure middleware is added before handlers:
```go
app.Use(logging.FiberMiddleware(logger))
app.Post("/api/v1/write", WriteHandler)
```

### Problem: Logs buffered, appearing late

**Solution**: Flush regularly or use sync writes:
```go
// Flush every minute
ticker := time.NewTicker(1 * time.Minute)
go func() {
    for range ticker.C {
        logger.Sync()
    }
}()
```

## License

Internal package for Soltix project.
