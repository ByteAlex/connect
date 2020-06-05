package discord4j.connect.rsocket.global;

import discord4j.common.operator.RateLimitOperator;
import discord4j.rest.request.GlobalRateLimiter;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public class SafeGlobalRateLimiter implements GlobalRateLimiter {

    private final RateLimitOperator<Integer> operator;

    private final AtomicLong limitedUntil = new AtomicLong();

    public SafeGlobalRateLimiter(int capacity, Duration refillPeriod, Scheduler delayScheduler) {
        this.operator = new RateLimitOperator<>(capacity, refillPeriod, delayScheduler);
    }

    @Override
    public Mono<Void> rateLimitFor(Duration duration) {
        return Mono.fromRunnable(() -> limitedUntil.set(System.nanoTime() + duration.toNanos()));
    }

    @Override
    public Mono<Duration> getRemaining() {
        return Mono.fromCallable(() -> Duration.ofNanos(limitedUntil.get() - System.nanoTime()));
    }

    @Override
    public <T> Flux<T> withLimiter(Publisher<T> stage) {
        return Mono.just(0)
                .transform(operator)
                .then(getRemaining())
                .filter(delay -> delay.getSeconds() > 0)
                .flatMapMany(delay -> Mono.delay(delay).flatMapMany(tick -> Flux.from(stage)))
                .switchIfEmpty(stage);
    }
}
