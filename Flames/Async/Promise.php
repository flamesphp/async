<?php
declare(strict_types=1);

namespace Flames\Async;

/*
 * Pure-PHP fallback Promise.
 *
 * The C extension registers Flames\Async\Async\Promise (not this namespace),
 * so there is never a conflict regardless of whether the extension is loaded,
 * outdated, or absent.  This class is always available for the PHP-pure path.
 */
final class Promise
{
    // ── State constants ────────────────────────────────────────────────────
    public const PENDING  = 0;
    public const RESOLVED = 1;
    public const REJECTED = 2;

    // ── Wait-mode constants (internal scheduler use) ───────────────────────
    /** @internal */ public const WAIT_DATA     = 0; // forFd      – read bytes from fd
    /** @internal */ public const WAIT_READABLE = 1; // forReadable – fd becomes readable
    /** @internal */ public const WAIT_WRITABLE = 2; // forWritable – fd becomes writable

    // ── Internal state ─────────────────────────────────────────────────────
    private int     $state    = self::PENDING;
    private mixed   $result   = null;
    private ?string $error    = null;

    /** @internal */ public int $fd       = -1;
    /** @internal */ public int $waitMode = self::WAIT_DATA;

    // ── Static factory methods (mirror the C extension API) ────────────────

    /**
     * Wait for data to arrive on $fd  (equivalent to C forFd).
     */
    public static function forFd(int $fd): static
    {
        $p           = new static();
        $p->fd       = $fd;
        $p->waitMode = self::WAIT_DATA;
        return $p;
    }

    /**
     * Wait until $fd is readable (non-blocking check only).
     */
    public static function forReadable(int $fd): static
    {
        $p           = new static();
        $p->fd       = $fd;
        $p->waitMode = self::WAIT_READABLE;
        return $p;
    }

    /**
     * Wait until $fd is writable (non-blocking check only).
     */
    public static function forWritable(int $fd): static
    {
        $p           = new static();
        $p->fd       = $fd;
        $p->waitMode = self::WAIT_WRITABLE;
        return $p;
    }

    // ── Settlement ─────────────────────────────────────────────────────────

    public function resolve(mixed $value): void
    {
        if ($this->state !== self::PENDING) {
            return;
        }
        $this->result = $value;
        $this->state  = self::RESOLVED;
    }

    public function reject(string $msg): void
    {
        if ($this->state !== self::PENDING) {
            return;
        }
        $this->error = $msg;
        $this->state = self::REJECTED;
    }

    public function isPending(): bool
    {
        return $this->state === self::PENDING;
    }

    // ── Internal accessors (used by Flames\Async\Service scheduler) ────────

    /** @internal */ public function getState(): int     { return $this->state; }
    /** @internal */ public function getResult(): mixed  { return $this->result; }
    /** @internal */ public function getError(): ?string { return $this->error; }
}
