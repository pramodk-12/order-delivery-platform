package com.reactiveplatform.product.streaming;

import com.mongodb.client.model.changestream.FullDocument;
import com.reactiveplatform.product.domain.Product;
import com.reactiveplatform.product.dto.PriceUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import java.time.Duration;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Live price changes, pushed by MongoDB itself - no polling.
 * <p>
 * A MongoDB CHANGE STREAM is a cursor that never ends: MongoDB pushes an event for every matching write
 * (it tails the replica-set oplog, which is why the database must run as a replica set).
 * <p>
 * The stream is made HOT and SHARED with {@code share()} (exercise 7):
 * <ul>
 *   <li>1 change stream per service instance, no matter how many browsers are connected</li>
 *   <li>opened when the first SSE client subscribes, closed when the last one leaves</li>
 *   <li>clients that connect later only see prices that change after they joined</li>
 * </ul>
 */
@Component
public class PriceChangeStream {

    private static final Logger log = LoggerFactory.getLogger(PriceChangeStream.class);

    private final Flux<PriceUpdate> updates;

    public PriceChangeStream(ReactiveMongoTemplate template) {
        this.updates = template.changeStream(Product.class)
                // update events only carry the changed fields; ask MongoDB to attach the full document
                .withOptions(options -> options.fullDocumentLookup(FullDocument.UPDATE_LOOKUP))
                .watchCollection(Product.class)
                // server-side filter: only updates that touched the price field
                .filter(where("operationType").is("update")
                        .and("updateDescription.updatedFields.price").exists(true))
                .listen()
                .mapNotNull(ChangeStreamEvent::getBody)
                .map(p -> new PriceUpdate(p.id(), p.sku(), p.name(), p.price(), p.updatedAt()))
                .doOnSubscribe(s -> log.info("First SSE client connected - opening MongoDB change stream"))
                .doOnCancel(() -> log.info("Last SSE client left - closing MongoDB change stream"))
                // if MongoDB restarts or fails over, reconnect instead of killing every client's stream
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(signal -> log.warn("Change stream failed, reconnecting (attempt {})",
                                signal.totalRetries() + 1, signal.failure())))
                .share();
    }

    public Flux<PriceUpdate> updates() {
        return updates;
    }
}
