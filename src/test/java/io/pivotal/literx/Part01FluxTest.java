package io.pivotal.literx;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vavr.control.Try.run;
import static java.lang.Thread.sleep;
import static reactor.core.scheduler.Schedulers.fromExecutor;

/**
 * Learn how to create Flux instances.
 *
 * @author Sebastien Deleuze
 * @see <a href="http://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html">Flux Javadoc</a>
 */
public class Part01FluxTest {

    Part01Flux workshop = new Part01Flux();

    private final Logger log = LoggerFactory.getLogger(Part01FluxTest.class);

//========================================================================================

    @Test
    public void empty() {
        Flux<String> flux = workshop.emptyFlux();

        StepVerifier.create(flux)
                .verifyComplete();
    }

//========================================================================================

    @Test
    public void fromValues() {
        Flux<String> flux = workshop.fooBarFluxFromValues();
        StepVerifier.create(flux)
                .expectNext("foo", "bar")
                .verifyComplete();
    }

//========================================================================================

    @Test
    public void fromList() {
        Flux<String> flux = workshop.fooBarFluxFromList();
        StepVerifier.create(flux)
                .expectNext("foo", "bar")
                .verifyComplete();
    }

//========================================================================================

    @Test
    public void error() {
        Flux<String> flux = workshop.errorFlux();
        StepVerifier.create(flux)
                .verifyError(IllegalStateException.class);
    }

//========================================================================================

    @Test
    public void countEach100ms() {
        Flux<Long> flux = workshop.counter();
        StepVerifier.create(flux)
                .expectNext(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
                .verifyComplete();
    }

    @Test
    public void createFlux() {
        final Flux<Object> flux = Flux.create(emitter -> {
            final AtomicInteger counter = new AtomicInteger();
            while (counter.incrementAndGet() < 101) {
                final int o = counter.get();
                emitter.next(o);
                try {
                    sleep(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("Sent [{}]", o);
            }
            emitter.complete();
        }, FluxSink.OverflowStrategy.BUFFER);
        final Flux<Object> stream = flux
//                .publishOn(Schedulers.newParallel("publisher", 4))
                .subscribeOn(Schedulers.newParallel("consumer", 4), false);
        stream
                .subscribe(o -> {
                    log.info("Got [{}]", o);
                    try {
                        sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        StepVerifier.create(stream)
                .expectNextCount(100)
                .verifyComplete();
    }

    @Test
    public void generateFlux() {
        final int count = 32;
        final long start = System.currentTimeMillis();
        final Flux<Object> done = Flux.create(progress -> {
            final Flux<Integer> source = Flux.generate(AtomicInteger::new, (counter, sink) -> {
                if (counter.get() >= count) {
                    sink.complete();
                    return counter;
                }
                sink.next(counter.incrementAndGet());
//                log.info("[{}] ready", counter.get());
//                run(() -> sleep(50));
                return counter;
            });
            final Flux<Integer> stream = source
                    .subscribeOn(fromExecutor(Executors.newFixedThreadPool(4)))
                    .publishOn(fromExecutor(Executors.newFixedThreadPool(4)), 4)
                    ;
            stream.subscribe(integer -> {
//                log.info("[{}] processed", integer);
                progress.next(integer);
                if (integer == 0) progress.complete();
//                run(() -> sleep(100));
            });

        });
        StepVerifier.create(done.take(count))
                .expectNextCount(count)
                .verifyComplete();
        log.info("Finished in [{}] ms", System.currentTimeMillis() - start);
    }

}
