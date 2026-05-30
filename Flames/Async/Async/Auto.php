<?php

namespace Flames\Async\Async;

use Flames\Autoload\C;

if (C::validAsync())
{
    /**
     * @auto
     */
    trait Auto
    {
        public static function async(\Closure $delegate): \Flames\Async\Async\Service\Task
        {
            echo 'c async';
            return \Flames\Async\Async\Service::async($delegate);
        }

        public static function await(mixed ...$tasks): mixed
        {
            return \Flames\Async\Async\Service::await(...$tasks);
        }
    }
} else {
    /**
     * @internal
     */
    trait Auto
    {
        public static function async(\Closure $delegate): \Flames\Async\Service\Task
        {
            echo 'php async';
            return \Flames\Async\Service::async($delegate);
        }

        public static function await(mixed ...$tasks): mixed
        {
            return \Flames\Async\Service::await(...$tasks);
        }
    }
}