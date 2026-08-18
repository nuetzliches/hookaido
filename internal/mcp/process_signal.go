package mcp

import "errors"

// errGracefulStopUnsupported reports that the platform has no way to ask a
// process to shut down cleanly, so the only stop available is a hard kill.
//
// Windows is the case that matters: os.Process.Signal maps every signal to
// TerminateProcess, so sending "SIGTERM" there is a kill with no drain and no
// shutdown hooks. Callers must not present that as a graceful stop.
var errGracefulStopUnsupported = errors.New("graceful stop is not supported on this platform; pass force to terminate the process")
