<?php
declare(strict_types=1);


namespace Flames\Async\Async;

use Flames\Autoload\C;

if (C::validAsync())
{
    /**
     * @auto
     */
    trait Auto
    {
        public static function fork(\Closure $delegate): \Flames\Async\Container
        {
            return new \Flames\Async\Container(
                \Flames\Async\Async\Service::fork($delegate)
            );
        }

        public static function wait(\Flames\Async\Container ...$containers): mixed
        {
            if (count($containers) === 1) {
                return \Flames\Async\Async\Service::wait($containers[0]->getInner());
            }
            $tasks = array_map(static fn($c) => $c->getInner(), $containers);
            return \Flames\Async\Async\Service::wait(...$tasks);
        }

        public static function async(\Closure $fn, bool $parallel = false): \Flames\Async\Container
        {
            if ($parallel) {
                return new \Flames\Async\Container(
                    \Flames\Async\Async\Service::fork($fn)
                );
            }
            return new \Flames\Async\Container(
                \Flames\Async\Async\Service::async($fn)
            );
        }

        public static function await(\Flames\Async\Container ...$containers): mixed
        {
            $results = [];
            foreach ($containers as $c) {
                $inner = $c->getInner();
                if ($inner instanceof \Flames\Async\Async\Promise) {
                    $results[] = \Flames\Async\Async\Service::await($inner);
                } else {
                    $results[] = \Flames\Async\Async\Service::wait($inner);
                }
            }
            return count($results) === 1 ? $results[0] : $results;
        }

        public static function run(): void
        {
            \Flames\Async\Async\Service::run();
        }
    }
} else {
    /**
     * @internal
     */
    trait Auto
    {
        public static function fork(\Closure $delegate): \Flames\Async\Container
        {
            return new \Flames\Async\Container(
                \Flames\Async\Service::fork($delegate)
            );
        }

        public static function wait(\Flames\Async\Container ...$containers): mixed
        {
            if (count($containers) === 1) {
                return \Flames\Async\Service::wait($containers[0]->getInner());
            }
            $tasks = array_map(static fn($c) => $c->getInner(), $containers);
            return \Flames\Async\Service::wait(...$tasks);
        }

        public static function async(\Closure $fn, bool $parallel = false): \Flames\Async\Container
        {
            if ($parallel) {
                return new \Flames\Async\Container(
                    \Flames\Async\Service::fork($fn)
                );
            }
            return new \Flames\Async\Container(
                \Flames\Async\Service::async($fn)
            );
        }

        public static function await(\Flames\Async\Container ...$containers): mixed
        {
            $results = [];
            foreach ($containers as $c) {
                $inner = $c->getInner();
                if ($inner instanceof \Flames\Async\Promise) {
                    $results[] = \Flames\Async\Service::await($inner);
                } else {
                    $results[] = \Flames\Async\Service::wait($inner);
                }
            }
            return count($results) === 1 ? $results[0] : $results;
        }

        public static function run(): void
        {
            \Flames\Async\Service::run();
        }
    }
}
