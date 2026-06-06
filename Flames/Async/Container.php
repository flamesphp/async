<?php
declare(strict_types=1);

namespace Flames\Async;

/**
 * Opaque wrapper returned by Async::fork() and Async::async().
 *
 * Encapsulates the underlying implementation handle regardless of which
 * backend is active:
 *
 *   • Flames\Async\Async\Service\Task  – C extension fork task
 *   • Flames\Async\Service\Task        – PHP-pure fork task
 *   • Flames\Async\Async\Promise       – C extension coroutine promise
 *   • Flames\Async\Promise             – PHP-pure coroutine promise
 *
 * Passing Container to Async::wait() / Async::await() instead of the raw
 * handle keeps userland code decoupled from the backend type.
 */
final class Container
{
    private mixed $inner;

    public function __construct(mixed $inner)
    {
        $this->inner = $inner;
    }

    /**
     * Returns the wrapped backend handle.
     *
     * @internal  Used by Async::wait() / Async::await() internals only.
     */
    public function getInner(): mixed
    {
        return $this->inner;
    }
}
