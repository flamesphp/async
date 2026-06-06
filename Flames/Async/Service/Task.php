<?php
declare(strict_types=1);

namespace Flames\Async\Service;

/**
 * Pure-PHP fallback implementation of \Flames\Async\Service\Task.
 *
 * Identical public API to the C extension's Task object. * Use this when the C extension cannot be installed.
 *
 * ── Execution strategy (auto-detected at construction time) ────────────────
 *
 *   FORK mode  (when pcntl + stream_socket_pair are available)
 *     Behaves identically to the C extension:
 *     – A real child process is forked immediately.
 *     – The closure executes in the child with a full copy of PHP state.
 *     – The return value is serialized (igbinary if available, else native)
 *       and sent back to the parent through a Unix socket pair.
 *     – isDone() polls with stream_select(timeout=0).
 *     – result() / await() block until the child finishes.
 *     – Multiple tasks truly run in parallel.
 *
 *   FIBER mode  (fallback – no pcntl)
 *     – Each task is wrapped in a Fiber and started immediately.
 *     – If the closure does not call Fiber::suspend(), it runs to completion
 *       synchronously inside the Service::async() call itself.
 *     – isDone() returns true as soon as the fiber has terminated.
 *     – result() / await() resume the fiber if it is still suspended.
 *     – ⚠ No true parallelism: tasks execute cooperatively one at a time.
 *       sleep(), DB queries, and any other blocking calls block the whole
 *       process.  Total time = SUM of all task durations, not MAX.
 *
 * ── Wire protocol (fork mode) ──────────────────────────────────────────────
 *   Byte 0x01 + serialized payload   → success
 *   Byte 0x02 + UTF-8 error message  → exception propagated to parent
 *
 * ── Limitations vs. C extension ───────────────────────────────────────────
 *   – PHP's exit() calls destructors; the C extension uses _exit() which
 *     bypasses them.  In fork mode the child calls exit(0), which may flush
 *     output buffers or run registered shutdown functions.  The child attempts
 *     to suppress this by discarding all output buffers before exiting.
 *   – Closures that capture non-serializable values (resources, other closures)
 *     will serialize correctly in both modes because the closure itself is
 *     NEVER serialized – in fork mode the closure is available as a copy-on-
 *     write duplicate; in fiber mode it stays in the same process.
 *   – Only the RETURN VALUE must be serializable (fork mode).
 */
final class Task
{
    private const STATUS_OK    = "\x01";
    private const STATUS_ERROR = "\x02";

    // ── Fork mode state ────────────────────────────────────────────────────
    /** @var resource|null */
    private mixed $forkSocket = null;
    private int   $forkPid    = -1;

    private static bool $isChildProcess = false;

    // ── Fiber mode state ───────────────────────────────────────────────────
    private ?\Fiber $fiber = null;

    // ── Shared state ───────────────────────────────────────────────────────
    private bool    $done     = false;
    private bool    $hasError = false;
    private mixed   $result   = null;
    private ?string $errorMsg = null;

    // ───────────────────────────────────────────────────────────────────────

    public function __construct(callable $fn)
    {
        if (self::canFork()) {
            $this->startFork($fn);
        } else {
            $this->startFiber($fn);
        }
    }

    public function __destruct()
    {
        // Child processes must never kill or close resources belonging to
        // sibling tasks that were inherited from the parent via fork.
        if (self::$isChildProcess) {
            return;
        }

        if ($this->forkSocket !== null) {
            @fclose($this->forkSocket);
            $this->forkSocket = null;
        }

        // Kill unawaited child to prevent zombies.
        if ($this->forkPid > 0) {
            if (function_exists('posix_kill')) {
                posix_kill($this->forkPid, SIGKILL);
            }
            pcntl_waitpid($this->forkPid, $status, WNOHANG);
            $this->forkPid = -1;
        }
    }

    // ── Public API (identical to C extension) ─────────────────────────────

    /**
     * Non-blocking check: returns true if the task has already finished.
     */
    public function isDone(): bool
    {
        if ($this->done) {
            return true;
        }

        if ($this->forkSocket !== null) {
            $read   = [$this->forkSocket];
            $write  = null;
            $except = null;
            if (stream_select($read, $write, $except, 0, 0) > 0) {
                $this->collectFork();
                return true;
            }
            return false;
        }

        if ($this->fiber !== null && $this->fiber->isTerminated()) {
            $this->collectFiberTerminated();
            return true;
        }

        return false;
    }

    /**
     * Blocks until the task finishes and returns its return value.
     * Throws \RuntimeException if the closure threw an exception.
     */
    public function result(): mixed
    {
        if (!$this->done) {
            if ($this->forkSocket !== null) {
                $this->collectFork();
            } else {
                $this->collectFiber();
            }
        }

        if ($this->hasError) {
            throw new \RuntimeException($this->errorMsg ?? 'async: unknown error');
        }

        return $this->result;
    }

    // ── Strategy detection ─────────────────────────────────────────────────

    private static function canFork(): bool
    {
        return function_exists('pcntl_fork')
            && function_exists('pcntl_waitpid')
            && function_exists('stream_socket_pair');
    }

    // ── Fork mode ──────────────────────────────────────────────────────────

    private function startFork(callable $fn): void
    {
        $pair = stream_socket_pair(
            STREAM_PF_UNIX,
            STREAM_SOCK_STREAM,
            STREAM_IPPROTO_IP
        );

        if ($pair === false) {
            // stream_socket_pair unexpectedly failed – degrade to fiber
            $this->startFiber($fn);
            return;
        }

        $pid = pcntl_fork();

        if ($pid < 0) {
            // fork failed – degrade to fiber
            fclose($pair[0]);
            fclose($pair[1]);
            $this->startFiber($fn);
            return;
        }

        if ($pid === 0) {
            /* ================================================================
             * CHILD PROCESS
             * ================================================================ */
            self::$isChildProcess = true;
            fclose($pair[0]); // child never reads

            try {
                $retval  = $fn();
                $payload = self::STATUS_OK . self::doSerialize($retval);
            } catch (\Throwable $e) {
                $payload = self::STATUS_ERROR . $e->getMessage();
            }

            fwrite($pair[1], $payload);
            fclose($pair[1]);

            // Discard output buffers so the parent does not receive double
            // output when PHP's exit() flushes them.
            while (ob_get_level() > 0) {
                ob_end_clean();
            }

            exit(0);
        }

        /* ====================================================================
         * PARENT PROCESS
         * ==================================================================== */
        fclose($pair[1]); // parent never writes
        $this->forkSocket = $pair[0];
        $this->forkPid    = $pid;
    }

    private function collectFork(): void
    {
        if ($this->done) {
            return;
        }

        $raw = stream_get_contents($this->forkSocket);
        fclose($this->forkSocket);
        $this->forkSocket = null;

        pcntl_waitpid($this->forkPid, $wstatus);
        $this->forkPid = -1;
        $this->done    = true;

        if ($raw === false || $raw === '') {
            $this->hasError = true;
            $this->errorMsg = 'async: child process terminated without a result';
            return;
        }

        $status  = $raw[0];
        $payload = substr($raw, 1);

        if ($status === self::STATUS_OK) {
            $this->result = self::doUnserialize($payload);
        } else {
            $this->hasError = true;
            $this->errorMsg = $payload;
        }
    }

    // ── Fiber mode ─────────────────────────────────────────────────────────

    private function startFiber(callable $fn): void
    {
        $this->fiber = new \Fiber(static function () use ($fn): mixed {
            return $fn();
        });

        try {
            $this->fiber->start();
        } catch (\Throwable $e) {
            $this->done     = true;
            $this->hasError = true;
            $this->errorMsg = $e->getMessage();
            return;
        }

        if ($this->fiber->isTerminated()) {
            $this->collectFiberTerminated();
        }
        // If the fiber suspended (e.g. via Fiber::suspend()), collectFiber()
        // will resume it when result() or await() is called.
    }

    private function collectFiber(): void
    {
        if ($this->done || $this->fiber === null) {
            return;
        }

        try {
            while ($this->fiber->isSuspended()) {
                $this->fiber->resume();
            }
        } catch (\Throwable $e) {
            $this->done     = true;
            $this->hasError = true;
            $this->errorMsg = $e->getMessage();
            return;
        }

        if ($this->fiber->isTerminated()) {
            $this->collectFiberTerminated();
        }
    }

    private function collectFiberTerminated(): void
    {
        $this->done = true;
        try {
            $this->result = $this->fiber->getReturn();
        } catch (\Throwable $e) {
            $this->hasError = true;
            $this->errorMsg = $e->getMessage();
        }
    }

    // ── Serialization (igbinary with native fallback) ──────────────────────

    private static function doSerialize(mixed $value): string
    {
        if (function_exists('igbinary_serialize')) {
            return (string) igbinary_serialize($value);
        }
        return serialize($value);
    }

    private static function doUnserialize(string $data): mixed
    {
        if (function_exists('igbinary_unserialize')) {
            return igbinary_unserialize($data);
        }
        return unserialize($data, ['allowed_classes' => true]);
    }
}
