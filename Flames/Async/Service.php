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
    // ── Public API (identical to C extension) ─────────────────────────────

    /**
     * Starts a new async task.
     *
     * In fork mode the child process begins immediately and runs concurrently.
     * In fiber mode the fiber is started immediately (runs until it yields
     * or completes, then control returns here).
     *
     * @param callable $fn Closure to execute asynchronously.
     *                      Captured `use` variables are available in both modes.
     * @return Task
     */
    public static function async(callable $fn): Task
    {
        return new Task($fn);
    }

    /**
     * Waits for one or more tasks and returns their results.
     *
     * Single task:   returns the value directly (not wrapped in an array).
     * Multiple tasks: returns an indexed array in argument order.
     *
     * Throws \RuntimeException if any task's closure threw an exception.
     *
     * In fork mode all children are already running concurrently;
     * this method just reads each pipe sequentially.
     * Total wall-time ≈ MAX(task durations).
     *
     * In fiber mode tasks are resumed/completed one at a time.
     * Total wall-time = SUM(task durations).
     *
     * @param Task ...$tasks One or more Task objects returned by async().
     * @return mixed          Single value or array of values.
     */
    public static function await(Task ...$tasks): mixed
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
}
