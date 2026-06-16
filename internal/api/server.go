package api

import (
	"context"
	"fmt"
	"io"

	"github.com/microsoft/typescript-go/internal/bundled"
	"github.com/microsoft/typescript-go/internal/lsp/lsproto"
	"github.com/microsoft/typescript-go/internal/project"
	"github.com/microsoft/typescript-go/internal/vfs/osvfs"
)

// StdioServerOptions configures the STDIO-based API server.
type StdioServerOptions struct {
	In                 io.ReadCloser
	Out                io.WriteCloser
	Err                io.Writer
	Cwd                string
	DefaultLibraryPath string
	// PipePath, if set, listens on a named pipe (Windows) or Unix domain
	// socket instead of using In/Out for communication.
	PipePath string
	// ShmName, if set, uses a shared memory region for communication instead
	// of pipes. The named region must already exist (created by the parent process).
	ShmName string
	// Callbacks specifies which filesystem operations should be delegated
	// to the client (e.g., "readFile", "fileExists"). Empty means no callbacks.
	Callbacks []string
	// Async enables JSON-RPC protocol with async connection handling.
	// When false (default), uses MessagePack protocol with sync connection.
	Async bool
}

// StdioServer runs an API session over STDIO using MessagePack protocol.
// This is the entry point for the synchronous STDIO-based API used by
// native TypeScript tooling integration.
type StdioServer struct {
	options *StdioServerOptions
}

// NewStdioServer creates a new STDIO-based API server.
func NewStdioServer(options *StdioServerOptions) *StdioServer {
	if options.Cwd == "" {
		panic("StdioServerOptions.Cwd is required")
	}

	return &StdioServer{
		options: options,
	}
}

// Run starts the server and blocks until the connection closes.
func (s *StdioServer) Run(ctx context.Context) error {
	fs := bundled.WrapFS(osvfs.FS())

	// Wrap the base FS with callbackFS if callbacks are requested
	var callbackFS *callbackFS
	if len(s.options.Callbacks) > 0 {
		callbackFS = newCallbackFS(fs, s.options.Callbacks)
		fs = callbackFS
	}

	projectSession := project.NewSession(&project.SessionInit{
		BackgroundCtx: ctx,
		Logger:        nil, // TODO: Add logging support
		FS:            fs,
		Options: &project.SessionOptions{
			CurrentDirectory:   s.options.Cwd,
			DefaultLibraryPath: s.options.DefaultLibraryPath,
			PositionEncoding:   lsproto.PositionEncodingKindUTF8,
			LoggingEnabled:     false,
		},
	})

	session := NewSession(projectSession, &SessionOptions{
		UseBinaryResponses: !s.options.Async, // Only msgpack uses binary responses
	})
	defer session.Close()

	// Create protocol and connection based on transport mode
	var conn Conn

	if s.options.ShmName != "" {
		// Shared memory mode: open existing region created by parent process
		conn, err := s.createShmConn(session)
		if err != nil {
			return err
		}

		if callbackFS != nil {
			callbackFS.SetConnection(ctx, conn)
		}
		return conn.Run(ctx)
	}

	// Pipe/stdio transport mode
	var transport Transport
	if s.options.PipePath != "" {
		t, err := NewPipeTransport(s.options.PipePath)
		if err != nil {
			return fmt.Errorf("failed to create pipe transport: %w", err)
		}
		defer t.Close()
		transport = t
	} else {
		t := NewStdioTransport(s.options.In, s.options.Out)
		defer t.Close()
		transport = t
	}

	// Accept connection from transport
	rwc, err := transport.Accept()
	if err != nil {
		return fmt.Errorf("failed to accept connection: %w", err)
	}

	if s.options.Async {
		protocol := NewJSONRPCProtocol(rwc)
		conn = NewAsyncConnWithProtocol(rwc, protocol, session)
	} else {
		protocol := NewMessagePackProtocol(rwc)
		conn = NewSyncConn(rwc, protocol, session)
	}

	if callbackFS != nil {
		callbackFS.SetConnection(ctx, conn)
	}

	return conn.Run(ctx)
}

// createShmConn opens the shared memory region and creates a SyncConn backed by it.
func (s *StdioServer) createShmConn(session *Session) (Conn, error) {
	// Read the region size from the control header (set by Node after creating it).
	// We need to know the size before opening. Use a default of 64MB which must
	// match what the Node side creates.
	const defaultShmSize = 64 * 1024 * 1024

	data, err := platformShmOpen(s.options.ShmName, defaultShmSize)
	if err != nil {
		return nil, fmt.Errorf("failed to open shared memory %q: %w", s.options.ShmName, err)
	}

	protocol, err := NewShmProtocol(data)
	if err != nil {
		platformShmClose(data)
		return nil, fmt.Errorf("failed to create shm protocol: %w", err)
	}

	// Use a shmCloser as the rwc — it unmaps the region on close
	closer := &shmCloser{data: data}
	return NewSyncConn(closer, protocol, session), nil
}

// shmCloser implements io.ReadWriteCloser for the SyncConn interface.
// Read/Write are no-ops since the ShmProtocol handles I/O directly.
type shmCloser struct {
	data []byte
}

func (c *shmCloser) Read([]byte) (int, error)  { return 0, io.EOF }
func (c *shmCloser) Write([]byte) (int, error) { return 0, nil }
func (c *shmCloser) Close() error              { return platformShmClose(c.data) }

