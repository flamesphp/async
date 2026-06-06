<?php


declare(strict_types=1);

namespace Flames\Async;

use Flames\Async\Service\Task;

/**
 * Pure-PHP fallback implementation of \Flames\Async\Async\Service.
 *
 * Identical public API to the C extension (\Flames\Async\Async\Service).
 * Use this when the C extension cannot be installed (shared hosting, etc.).
 *
 * ── Strategy auto-detection ────────────────────────────────────────────────
 *
 *   The Task object automatically picks the best strategy available:
 *
 *   1. FORK mode  (requires pcntl + stream_socket_pair)
 *      Real child processes — true parallelism, identical to the C extension.
 *      Total time = MAX(task durations).
 *
 *   2. FIBER mode  (PHP 8.1+ Fibers, always available)
 *      Cooperative execution — no true parallelism.
 *      Total time = SUM(task durations).
 *      ⚠ blocking calls (sleep, DB queries) still block the whole process.
 *
 * ── Usage ──────────────────────────────────────────────────────────────────
 *
 *   // Without C extension:
 *   require_once __DIR__ . '/../php/Service.php';
 *   require_once __DIR__ . '/../php/Service/Task.php';
 *
 *   use Flames\Async\Async\Service;
 *
 *   $type     = 'non-admin';
 *   $getUsers = Service::async(function() use ($type) {
 *       sleep(2);
 *       return ['users' => [], 'type' => $type];
 *   });
 *   $getConfigs = Service::async(function() {
 *       sleep(1);
 *       return ['debug' => false];
 *   });
 *
 *   [$users, $configs] = Service::await($getUsers, $getConfigs);
 *                                // fork mode: ~2 s
 *                                // fiber mode: ~3 s (sequential)
 *
 * ── Differences from the C extension ──────────────────────────────────────
 *
 *   • The C extension uses _exit() in the child, which skips PHP shutdown
 *     handlers.  This PHP version uses exit(0), which runs destructors.
 *     In fork mode the child discards all output buffers before exit to
 *     avoid double output, but registered shutdown functions still run.
 *
 *   • In fiber mode, blocking calls (sleep, I/O) block the entire process.
 *     The C extension never has this problem because it uses real processes.
 */
final class Service
{
    // ── Coroutine scheduler state ──────────────────────────────────────────

    /**
     * Each entry: ['fiber' => Fiber, 'inner' => Promise, 'outer' => Promise]
     *
     * @var array<int, array{fiber: \Fiber, inner: \Flames\Async\Promise, outer: \Flames\Async\Promise}>
     */
    private static array $waiters = [];

    // ── fork / wait API (process-based parallelism) ───────────────────────

    /**
     * Starts a new forked task (process-based).
     */
    public static function fork(callable $fn): Task
    {
        return new Task($fn);
    }

    /**
     * Waits for one or more forked tasks and returns their results.
     */
    public static function wait(Task ...$tasks): mixed
    {
        if (count($tasks) === 1) {
            return $tasks[0]->result();
        }

        $results = [];
        foreach ($tasks as $task) {
            $results[] = $task->result();
        }
        return $results;
    }

    // ── async / await / run API (Fiber-based coroutines) ──────────────────

    /**
     * Wraps $fn in a Fiber and starts it immediately.
     *
     * Returns a Promise that resolves when $fn returns or rejects when it
     * throws.  The closure may call Service::await() internally to yield
     * control back to the scheduler while waiting for other Promises.
     *
     * Multiple async() calls run cooperatively: each Fiber executes until it
     * hits an unresolved await(), at which point the scheduler picks up the
     * next ready Fiber.
     *
     * @param callable $fn Closure to execute as a coroutine.
     * @return \Flames\Async\Promise Completion promise.
     */
    public static function async(callable $fn): \Flames\Async\Promise
    {
        $outer = new \Flames\Async\Promise();

        $fiber = new \Fiber(static function () use ($fn): mixed {
            return $fn();
        });

        try {
            $fiber->start();
        } catch (\Throwable $e) {
            $outer->reject($e->getMessage());
            return $outer;
        }

        self::stepFiber($fiber, $outer);
        return $outer;
    }

    /**
     * Awaits one or more Promises.
     *
     * Single promise  → returns the resolved value directly.
     * Multiple promises → returns an indexed array in argument order.
     *
     * All coroutines advance concurrently while the loop runs.
     *
     * @throws \RuntimeException On any rejection.
     */
    public static function await(\Flames\Async\Promise ...$promises): mixed
    {
        if (count($promises) === 1) {
            return self::awaitOne($promises[0]);
        }
        $results = [];
        foreach ($promises as $p) {
            $results[] = self::awaitOne($p);
        }
        return $results;
    }

    private static function awaitOne(\Flames\Async\Promise $promise): mixed
    {
        if ($promise->getState() === \Flames\Async\Promise::RESOLVED) {
            return $promise->getResult();
        }

        if ($promise->getState() === \Flames\Async\Promise::REJECTED) {
            throw new \RuntimeException($promise->getError() ?? 'Promise rejected');
        }

        if (\Fiber::getCurrent() !== null) {
            // Inside a Fiber: yield to the scheduler, which will resume us
            // once $promise settles.
            $resumed = \Fiber::suspend($promise);

            if ($promise->getState() === \Flames\Async\Promise::REJECTED) {
                throw new \RuntimeException($promise->getError() ?? 'Promise rejected');
            }

            return $resumed;
        }

        // Main thread: drive the loop until this specific promise settles.
        while ($promise->isPending() && !empty(self::$waiters)) {
            self::tick(50);
        }

        if ($promise->getState() === \Flames\Async\Promise::RESOLVED) {
            return $promise->getResult();
        }

        if ($promise->getState() === \Flames\Async\Promise::REJECTED) {
            throw new \RuntimeException($promise->getError() ?? 'Promise rejected');
        }

        throw new \RuntimeException(
            'Flames\\Async\\Service::await(): promise did not settle'
        );
    }

    /**
     * Drives the scheduler until all pending coroutines have completed.
     *
     * Call this once after scheduling all your async() tasks if you do not
     * individually await each one.
     */
    public static function run(): void
    {
        while (!empty(self::$waiters)) {
            self::tick(50);
        }
    }

    // ── Scheduler internals ────────────────────────────────────────────────

    /**
     * Inspects a Fiber after start() or resume() and either marks the outer
     * promise as settled (fiber terminated) or registers the (fiber, inner
     * promise) pair in the waiter list.
     */
    private static function stepFiber(\Fiber $fiber, \Flames\Async\Promise $outer): void
    {
        if ($fiber->isTerminated()) {
            if ($outer->isPending()) {
                try {
                    $outer->resolve($fiber->getReturn());
                } catch (\Throwable $e) {
                    $outer->reject($e->getMessage());
                }
            }
            return;
        }

        $inner = $fiber->getSuspendedValue();

        if (!($inner instanceof \Flames\Async\Promise)) {
            // Unknown suspend value – resume with null immediately.
            try {
                $fiber->resume(null);
            } catch (\Throwable $e) {
                if ($outer->isPending()) {
                    $outer->reject($e->getMessage());
                }
                return;
            }
            self::stepFiber($fiber, $outer);
            return;
        }

        // Already settled?  Resume right away.
        if (!$inner->isPending()) {
            self::resumeFromPromise($fiber, $outer, $inner);
            return;
        }

        // Still pending – park in the waiter list.
        self::$waiters[] = ['fiber' => $fiber, 'inner' => $inner, 'outer' => $outer];
    }

    /**
     * Resumes $fiber with the settled value of $inner and calls stepFiber()
     * again to advance the fiber to its next suspension point or completion.
     */
    private static function resumeFromPromise(
        \Fiber $fiber,
        \Flames\Async\Promise $outer,
        \Flames\Async\Promise $inner
    ): void {
        if ($inner->getState() === \Flames\Async\Promise::RESOLVED) {
            try {
                $fiber->resume($inner->getResult());
            } catch (\Throwable $e) {
                if ($outer->isPending()) {
                    $outer->reject($e->getMessage());
                }
                return;
            }
        } else {
            $msg = $inner->getError() ?? 'Promise rejected';
            try {
                $fiber->throw(new \RuntimeException($msg));
            } catch (\Throwable $e) {
                if ($outer->isPending()) {
                    $outer->reject($e->getMessage());
                }
                return;
            }
        }

        self::stepFiber($fiber, $outer);
    }

    /**
     * One iteration of the event loop.
     *
     * Pass 1 – drain waiters whose inner promise was resolved manually
     *          (e.g. by user code calling $promise->resolve()).
     *
     * Pass 2 – for fd-backed promises, use stream_select() to check I/O
     *          readiness and resolve them when the fd is ready.
     */
    private static function tick(int $timeoutMs): void
    {
        if (empty(self::$waiters)) {
            return;
        }

        // ── Pass 1: manually-resolved promises (fd < 0) ───────────────────
        foreach (self::$waiters as $i => $w) {
            if ($w['inner']->fd >= 0) {
                continue;
            }
            if ($w['inner']->isPending()) {
                continue;
            }
            unset(self::$waiters[$i]);
            self::resumeFromPromise($w['fiber'], $w['outer'], $w['inner']);
        }

        if (empty(self::$waiters)) {
            return;
        }

        // ── Pass 2: fd-backed promises via stream_select ──────────────────
        $readStreams  = [];
        $writeStreams = [];
        // stream-resource-id => waiter-index
        $mapRead  = [];
        $mapWrite = [];

        foreach (self::$waiters as $i => $w) {
            $fd = $w['inner']->fd;
            if ($fd < 0) {
                continue;
            }

            // Wrap the integer fd in a PHP stream resource (Linux-only, best-effort).
            $stream = @fopen('php://fd/' . $fd, 'r+');
            if ($stream === false) {
                // Cannot wrap fd – resolve immediately so the Fiber is not stuck.
                $w['inner']->resolve(null);
                unset(self::$waiters[$i]);
                self::resumeFromPromise($w['fiber'], $w['outer'], $w['inner']);
                continue;
            }
            stream_set_blocking($stream, false);

            $sid = (int) $stream;
            if ($w['inner']->waitMode === \Flames\Async\Promise::WAIT_WRITABLE) {
                $writeStreams[$sid] = $stream;
                $mapWrite[$sid]     = $i;
            } else {
                $readStreams[$sid] = $stream;
                $mapRead[$sid]     = $i;
            }
        }

        if (!empty($readStreams) || !empty($writeStreams)) {
            $r      = array_values($readStreams);
            $wS     = array_values($writeStreams);
            $except = null;
            $tvSec  = intdiv($timeoutMs, 1000);
            $tvUsec = ($timeoutMs % 1000) * 1000;

            $n = stream_select($r, $wS, $except, $tvSec, $tvUsec);

            if ($n > 0) {
                foreach ($r as $s) {
                    $sid = (int) $s;
                    $idx = $mapRead[$sid] ?? null;
                    if ($idx === null || !isset(self::$waiters[$idx])) {
                        @fclose($s);
                        continue;
                    }
                    $w     = self::$waiters[$idx];
                    $inner = $w['inner'];
                    unset(self::$waiters[$idx]);

                    if ($inner->waitMode === \Flames\Async\Promise::WAIT_DATA) {
                        $data = @stream_get_contents($s);
                        $inner->resolve($data !== false ? $data : null);
                    } else {
                        $inner->resolve(true);
                    }
                    @fclose($s);
                    self::resumeFromPromise($w['fiber'], $w['outer'], $inner);
                }

                foreach ($wS as $s) {
                    $sid = (int) $s;
                    $idx = $mapWrite[$sid] ?? null;
                    if ($idx === null || !isset(self::$waiters[$idx])) {
                        @fclose($s);
                        continue;
                    }
                    $w = self::$waiters[$idx];
                    unset(self::$waiters[$idx]);
                    $w['inner']->resolve(true);
                    @fclose($s);
                    self::resumeFromPromise($w['fiber'], $w['outer'], $w['inner']);
                }
            } else {
                // No fd was ready – close the temporary stream references.
                foreach (array_merge($readStreams, $writeStreams) as $s) {
                    @fclose($s);
                }
            }
        }

        self::$waiters = array_values(self::$waiters);
    }
}
