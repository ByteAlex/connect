/*
 * This file is part of Discord4J.
 *
 * Discord4J is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discord4J is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Discord4J. If not, see <http://www.gnu.org/licenses/>.
 */

package discord4j.connect.common;

import discord4j.discordjson.json.gateway.Dispatch;
import discord4j.discordjson.json.gateway.Opcode;
import discord4j.gateway.GatewayClient;
import discord4j.gateway.SessionInfo;
import discord4j.gateway.ShardInfo;
import discord4j.gateway.json.GatewayPayload;
import discord4j.gateway.json.ShardAwareDispatch;
import discord4j.gateway.json.ShardGatewayPayload;
import discord4j.gateway.payload.PayloadReader;
import discord4j.gateway.payload.PayloadWriter;
import discord4j.gateway.retry.GatewayStateChange;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.reactivestreams.Publisher;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A {@link GatewayClient} implementation that connects to a {@link PayloadSource} and {@link PayloadSink} to
 * communicate messages across multiple nodes. This implementation does not establish any connection to Discord
 * Gateway and does not provide meaningful state about the actual Gateway connection.
 */
public class DownstreamGatewayClient implements GatewayClient {

    private static final Logger log = Loggers.getLogger(DownstreamGatewayClient.class);
    private static final Logger senderLog = Loggers.getLogger("discord4j.gateway.protocol.sender");
    private static final Logger receiverLog = Loggers.getLogger("discord4j.gateway.protocol.receiver");

    private final EmitterProcessor<Dispatch> dispatch = EmitterProcessor.create(false);
    private final EmitterProcessor<GatewayPayload<?>> receiver = EmitterProcessor.create(false);
    private final EmitterProcessor<GatewayPayload<?>> sender = EmitterProcessor.create(false);

    private final AtomicInteger lastSequence = new AtomicInteger(0);
    private final AtomicInteger shardCount = new AtomicInteger();
    private final AtomicReference<String> sessionId = new AtomicReference<>("");

    private final FluxSink<Dispatch> dispatchSink;
    private final FluxSink<GatewayPayload<?>> receiverSink;
    private final FluxSink<GatewayPayload<?>> senderSink;

    private final PayloadSink sink;
    private final PayloadSource source;
    private final PayloadReader payloadReader;
    private final PayloadWriter payloadWriter;
    private final ShardInfo initialShardInfo;
    private final boolean filterByIndex;
    private final MonoProcessor<Void> closeFuture = MonoProcessor.create();

    public DownstreamGatewayClient(ConnectGatewayOptions gatewayOptions) {
        this.sink = gatewayOptions.getPayloadSink();
        this.source = gatewayOptions.getPayloadSource();
        this.payloadReader = gatewayOptions.getPayloadReader();
        this.payloadWriter = gatewayOptions.getPayloadWriter();
        this.initialShardInfo = gatewayOptions.getIdentifyOptions().getShardInfo();
        this.filterByIndex = initialShardInfo.getIndex() != 0 || initialShardInfo.getCount() != 1;
        this.shardCount.set(gatewayOptions.getIdentifyOptions().getShardCount());
        this.dispatchSink = dispatch.sink(FluxSink.OverflowStrategy.LATEST);
        this.receiverSink = receiver.sink(FluxSink.OverflowStrategy.LATEST);
        this.senderSink = sender.sink(FluxSink.OverflowStrategy.LATEST);
    }

    @Override
    public Mono<Void> execute(String gatewayUrl) {
        return Mono.defer(() -> {
            // Receive from upstream -> Send to user
            Mono<Void> inboundFuture = source.receive(
                    inPayload -> {
                        if (receiverSink.isCancelled()) {
                            return Mono.error(new IllegalStateException("Sender was cancelled"));
                        }

                        if (filterByIndex && inPayload.getShard().getIndex() != initialShardInfo.getIndex()) {
                            return Mono.empty();
                        }
                        sessionId.set(inPayload.getSession().getId());
                        shardCount.set(inPayload.getShard().getCount());

                        logPayload(receiverLog, inPayload);

                        return Flux.from(payloadReader.read(Unpooled.wrappedBuffer(inPayload.getPayload().getBytes(StandardCharsets.UTF_8))))
                                .map(payload -> new ShardGatewayPayload<>(payload, inPayload.getShard().getIndex()))
                                .doOnNext(receiverSink::next)
                                .then();
                    })
                    .then();

            Mono<Void> receiverFuture = receiver.map(this::updateSequence)
                    .doOnNext(this::handlePayload)
                    .then();

            // Receive from user -> Send to upstream
            Mono<Void> senderFuture =
                    sink.send(sender.flatMap(payload -> Flux.from(payloadWriter.write(payload))
                            .map(buf -> buf.toString(StandardCharsets.UTF_8))
                            .map(str -> {
                                ConnectPayload cp = new ConnectPayload(getShardInfo(payload), getSessionInfo(), str);
                                logPayload(senderLog, cp);
                                return cp;
                            })))
                            .subscribeOn(Schedulers.newSingle("payload-sender"))
                            .then();

            return Mono.zip(inboundFuture, receiverFuture, senderFuture, closeFuture)
                    // a downstream client should only signal "connected" state on subscription
                    // TODO: improve signalling state for this client
                    .doOnSubscribe(s -> dispatchSink.next(GatewayStateChange.connected()))
                    .doOnError(t -> log.error("Gateway client error: {}", t.toString()))
                    .doOnCancel(() -> close(false))
                    .retryBackoff(Long.MAX_VALUE, Duration.ofSeconds(2), Duration.ofSeconds(30))
                    .then();
        });
    }

    private void logPayload(Logger logger, ConnectPayload payload) {
        if (logger.isTraceEnabled()) {
            logger.trace(payload.toString().replaceAll("(\"token\": ?\")([A-Za-z0-9._-]*)(\")", "$1hunter2$3"));
        }
    }

    private ShardInfo getShardInfo(GatewayPayload<?> payload) {
        if (payload instanceof ShardGatewayPayload) {
            ShardGatewayPayload<?> shardPayload = (ShardGatewayPayload<?>) payload;
            return new ShardInfo(shardPayload.getShardIndex(), getShardCount());
        }
        return initialShardInfo;
    }

    private SessionInfo getSessionInfo() {
        return new SessionInfo(getSessionId(), getSequence());
    }

    private GatewayPayload<?> updateSequence(GatewayPayload<?> payload) {
        if (payload.getSequence() != null) {
            lastSequence.set(payload.getSequence());
        }
        return payload;
    }

    private void handlePayload(GatewayPayload<?> payload) {
        if (Opcode.DISPATCH.equals(payload.getOp())) {
            if (payload.getData() != null) {
                if (payload instanceof ShardGatewayPayload) {
                    ShardGatewayPayload<?> shardPayload = (ShardGatewayPayload<?>) payload;
                    dispatchSink.next(new ShardAwareDispatch(shardPayload.getShardIndex(), getShardCount(),
                            (Dispatch) payload.getData()));
                } else {
                    dispatchSink.next((Dispatch) payload.getData());
                }
            }
        }
    }

    @Override
    public int getShardCount() {
        return shardCount.get();
    }

    @Override
    public boolean isConnected() {
        // TODO: add support for DownstreamGatewayClient::isConnected
        return true;
    }

    @Override
    public Duration getResponseTime() {
        // TODO: add support for DownstreamGatewayClient::getResponseTime
        return Duration.ZERO;
    }

    @Override
    public Mono<Void> close(boolean allowResume) {
        return Mono.fromRunnable(() -> {
            senderSink.complete();
            closeFuture.onComplete();
        });
    }

    @Override
    public Flux<Dispatch> dispatch() {
        return dispatch.doOnSubscribe(s -> log.info("Subscribed to dispatch sequence"));
    }

    @Override
    public Flux<GatewayPayload<?>> receiver() {
        return receiver;
    }

    @Override
    public <T> Flux<T> receiver(Function<ByteBuf, Publisher<? extends T>> mapper) {
        // have to convert to ByteBuf since we don't directly use it at the downstream level
        return receiver.flatMap(payloadWriter::write).flatMap(mapper);
    }

    @Override
    public FluxSink<GatewayPayload<?>> sender() {
        return senderSink;
    }

    @Override
    public Mono<Void> sendBuffer(Publisher<ByteBuf> publisher) {
        return Flux.from(publisher).flatMap(payloadReader::read).doOnNext(senderSink::next).then();
    }

    @Override
    public String getSessionId() {
        return sessionId.get();
    }

    @Override
    public int getSequence() {
        return lastSequence.get();
    }
}
