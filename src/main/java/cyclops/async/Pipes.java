package cyclops.async;

import com.aol.cyclops2.react.threads.SequentialElasticPools;
import com.aol.cyclops2.types.reactive.ValueSubscriber;
import cyclops.async.adapters.Adapter;
import cyclops.collections.box.LazyImmutable;
import cyclops.collections.immutable.PersistentMapX;
import cyclops.collections.mutable.ListX;
import cyclops.collections.tuple.Tuple;
import cyclops.collections.tuple.Tuple0;
import cyclops.control.*;
import cyclops.control.lazy.Eval;
import cyclops.control.lazy.Maybe;
import cyclops.stream.FutureStream;
import cyclops.stream.ReactiveSeq;
import cyclops.stream.Spouts;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 * Pipes : Stores and manages cyclops2-react Adapters for cross-thread communication
 * 
 * Connected Streams will not be able to complete collect or reduce style methods unless the underlying Adapter for data transfer is closed.
 * I.e. connected Streams remain connected until lazy the Adapter is closed, or they disconnect (due to a limit for example).
 * 
 * <pre>
 * {@close 
 * 
 *      //create a Pipes instance to manage inter-thread communication
 *      Pipes<String, Integer> bus = Pipes.of();
 *      
 *      //register a non-blocking queue for data transfer
        bus.register("reactor", QueueFactories.<Integer>boundedNonBlockingQueue(1000)
                                              .build());
        
        //publish data to transfer queue
        bus.publishTo("reactor",ReactiveSeq.of(10,20,30));
        
        //close transfer queue - connected Streams will disconnect once all
        //data transferred
        bus.close("reactor");
        
        
        //on another thread
       
       //connect to our transfer queue
       LazyFutureStream<Integer> futureStream =  bus.futureStream("reactor", new LazyReact(10,10)).get();
       
       
       //read data and print it out the console.
       futureStream.transform(i->"fan-out to handle blocking I/O:" + Thread.currentThread().getId() + ":"+i)
                   .forEach(System.out::println);
 * 
 * }
 * </pre>
 * 
 * @see Adapter
 * 
 * @author johnmcclean
 * 
 * 
 * @param <K> Key type
 * @param <V> Value type transferred via managed Adapters
 *
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Pipes<K, V> {

    private final ConcurrentMap<K, Adapter<V>> registered = new ConcurrentHashMap<>();

    /**
     * @return Numer of registered adapters
     */
    public int size() {
        return registered.size();
    }

    /**
     * @return Persistent transform of all registered adapters
     */
    public PersistentMapX<K, Adapter<V>> registered() {
        return PersistentMapX.fromMap(registered);
    }

    /**
     * @return Construct an zero Pipes instance
     */
    public static <K, V> Pipes<K, V> of() {
        return new Pipes<>();
    }

    /**
     * Construct a Pipes instance to manage a predefined Map of Adapaters
     * 
     * @param registered Adapters to register
     * @return Pipes instance to manage provided Adapters
     */
    public static <K, V> Pipes<K, V> of(final Map<K, Adapter<V>> registered) {
        Objects.requireNonNull(registered);
        final Pipes<K, V> pipes = new Pipes<>();
        pipes.registered.putAll(registered);
        return pipes;
    }

    /**
     * Push a singleUnsafe value synchronously into the Adapter identified by the supplied Key,
     * if it exists
     * 
     * <pre>
     * {@code 
     * 
     *     Pipes<String,String> pipes = Pipes.of();
     *     pipes.register("hello", new Queue<String>());
           
           pipes.push("hello", "world");
            
           //on another thread 
           pipes.reactiveSeq("hello")
                .get()
                .forEach(System.out::println);
     * 
     * }
     * </pre>
     * 
     * @param key Adapter key
     * @param value Value to push to Adapter
     */
    public void push(final K key, final V value) {
        Optional.ofNullable(registered.get(key))
                .ifPresent(a -> a.offer(value));
    }

    /**
     * Get the Adapter identified by the specified key
     * 
     * <pre>
     * {@code 
     *    //close an adapter
     *   pipes.get("adapter-key")
     *        .transform(a->a.close())
     *        .orElse(false); //Maybe is lazy - trigger action
     *   
     * }
     * </pre>
     * 
     * 
     * @param key : Adapter identifier
     * @return selected Queue
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Option<Adapter<V>> get(final K key) {
        return Option.ofNullable((Adapter) registered.get(key));
    }

    /**
     * Create a FutureStream using default Parallelism from the Adapter
     * identified by the provided key
     * 
     * @see LazyReact#parallelBuilder()
     * 
     * <pre>
     * {@code 
     *  Pipes<String, Integer> bus = Pipes.of();
        bus.register("reactor", QueueFactories.<Integer>boundedNonBlockingQueue(1000)
                                              .build());
        
        bus.publishTo("reactor",ReactiveSeq.of(10,20,30));
        
        bus.close("reactor");
        
        
        //on another thread
       List<String> res =  bus.futureStream("reactor")
                              .get()
                              .transform(i->"fan-out to handle blocking I/O:" + Thread.currentThread().getId() + ":"+i)
                               .toList();
       System.out.println(res);
       
        assertThat(res.size(),equalTo(3));
     * 
     * 
     * }
     * </pre>
     * 
     * 
     * 
     * @param key : Adapter identifier
     * @return LazyFutureStream from selected Queue
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Option<FutureStream<V>> futureStream(final K key) {
        return get(key).map(a -> a.futureStream());
    }

    /**
     * Create a FutureStream using the provided LazyReact futureStream builder
     * from the Adapter identified by the provided Key
     * 
     * <pre>
     * {@code 
     *  Pipes<String, Integer> bus = Pipes.of();
        bus.register("reactor", QueueFactories.<Integer>boundedNonBlockingQueue(1000)
                                              .build());
        
        bus.publishTo("reactor",ReactiveSeq.of(10,20,30));
        
        bus.close("reactor");
        
        
        //on another thread
       List<String> res =  bus.futureStream("reactor", new LazyReact(10,10))
                              .get()
                              .transform(i->"fan-out to handle blocking I/O:" + Thread.currentThread().getId() + ":"+i)
                               .toList();
       System.out.println(res);
       
        assertThat(res.size(),equalTo(3));
     * 
     * 
     * }
     * </pre> 
     * 
     * 
     * @param key : Adapter identifier
     * @param builder LazyReact futureStream builder
     * @return LazyFutureStream from selected Queue
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Option<FutureStream<V>> futureStream(final K key, final LazyReact builder) {

        return get(key).map(a -> a.futureStream(builder));
    }

    /**
     * Create a ReactiveSeq from the Adapter identified by the provided Key
     * 
     * <pre>
     * {@code 
     *  Queue<String> q = new Queue<>();
        pipes.register("data-queue", q);
        pipes.push("data-queue", "world");
        
        //on a separate thread
        ReactiveSeq<String> reactiveStream = pipes.reactiveSeq("data-queue");
        reactiveStream.forEach(System.out::println);
        //"world"
       
      
        
     * 
     * }
     * </pre>
     * 
     * 
     * @param key : Adapter identifier
     * @return {@link ReactiveSeq} from selected Queue
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Option<ReactiveSeq<V>> reactiveSeq(final K key) {
        return get(key).map(a -> a.stream());
    }

    /**
     * Extract the next x values from the Adapter identified by the provided Key
     * If the Adapter doesn't exist an zero List is returned
     * 
     * <pre>
     * {@code 
     *  Queue<String> q = new Queue<>();
        pipes.register("hello", q);
        pipes.push("hello", "world");
        pipes.push("hello", "world2");
        pipes.push("hello", "world3");
        pipes.push("hello", "world4");
        
        //on a separate thread
        pipes.xValues("hello",2) //ListX.of("world","world2")
        pipes.xValues("hello",2) //ListX.of("world3","world4")
     * 
     * 
     * }
     * </pre>
     * 
     * 
     * @param key : Adapter identifier
     * @param x Number of elements to return
     * @return List of the next x elements from the Adapter identified by the provided key
     */
    public ListX<V> xValues(final K key, final long x) {

        return get(key).map(a -> a.stream()
                                    .limit(x)
                                    .toListX())
                       .orElse(ListX.empty());
    }

    /**
     * Extract one value from the selected pipe, if it exists
     * 
     * @param key : Adapter identifier
     * @return Maybe containing next value from the Adapter identified by the provided key
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Option<V> oneValue(final K key) {
        final ValueSubscriber<V> sub = ValueSubscriber.subscriber();
        return get(key).peek(a -> a.stream()
                                   .subscribe(sub))
                       .flatMap(a -> sub.toMaybe());
    }

    /**
     * Extact one value from the selected pipe or an error if it doesn't exist (NoSuchElementException).
     * 
     * <pre>
     * {@code 
     *  
     *  Queue<String> q = new Queue<>();
        pipes.register("hello", q);
        pipes.push("hello", "world");
        pipes.push("hello", "world2");
        
       pipes.oneOrError("hello")
            .get() //"world"
       
     * }
     * </pre>
     * @param key : Adapter identifier
     * @return Xor containing lazy a NoSuchElementException an Adapter with the specified key does not exist,
     *            or the next value from that Adapter
     */
    public Either<Throwable, V> oneOrError(final K key) {
        final ValueSubscriber<V> sub = ValueSubscriber.subscriber();
        return get(key).peek(a -> a.stream()
                                   .subscribe(sub))
                       .map(a -> sub.toXor())
                       .orElse(Either.left(new NoSuchElementException(
                                                                        "no adapter for key " + key)));
    }

    /**
     * Extact one value from the selected pipe or an zero Maybe if it doesn't exist. Currently only Adapter's and not Publishers
     * are managed by Pipes so Publisher errors are not propagated (@see {@link Pipes#oneValue(Object)} or @see {@link Pipes#oneOrError(Object)} is better at the moment.
     * 
     *  <pre>
     *  {@code 
     *  Queue<String> q = new Queue<>();
        pipes.register("hello", q);
        pipes.push("hello", "world");
        pipes.push("hello", "world2");
      
        
        pipes.oneValueOrError("hello",Throwable.class).get(); //Try["world"]
       
     *  }
     *  </pre>
     * 
     * @param key
     * @param classes
     * @return
     */
    @Deprecated //errors aren't propagated across Adapters (at least without continuations)
    public <X extends Throwable> Option<Try<V, X>> oneValueOrError(final K key, final Class<X>... classes) {
        final ValueSubscriber<V> sub = ValueSubscriber.subscriber();
        return get(key).peek(a -> a.stream()
                                   .subscribe(sub))
                       .map(a -> sub.toTry(classes));
    }

    /**
     * Extact one value from the selected pipe or an zero Maybe if it doesn't exist. Currently only Adapter's and not Publishers
     * are managed by Pipes so Publisher errors are not propagated (@see {@link Pipes#oneValue(Object)} or @see {@link Pipes#oneOrError(Object)} is better at the moment.
     * 
     *  <pre>
     *  {@code 
     *  Queue<String> q = new Queue<>();
        pipes.register("hello", q);
        pipes.push("hello", "world");
        pipes.push("hello", "world2");
      
        
        pipes.oneValueOrError("hello").get(); //Try["world"]
       
     *  }
     *  </pre>
     * 
     * @param key : Adapter identifier
     * @return
     */
    @Deprecated //errors aren't propagated across Adapters (at least without continuations)
    public Option<Try<V, Throwable>> oneValueOrError(final K key) {
        final ValueSubscriber<V> sub = ValueSubscriber.subscriber();
        return get(key).peek(a -> a.stream()
                                   .subscribe(sub))
                       .map(a -> sub.toTry(Throwable.class));
    }

    /**
     * Asynchronously extract a value from the Adapter identified by the provided Key
     * <pre>
     * {@code 
     *  Queue<String> q = new Queue<>();
        pipes.register("hello", q);
        pipes.push("hello", "world");
        pipes.push("hello", "world2");
       
        pipes.oneOrErrorAsync("hello", ex) // Future.ofResult("world")
     * 
     * }
     * </pre>
     * 
     * 
     * @param key : Adapter identifier
     * @param ex Executor to extract value from Adapter from on
     * @return Future containing lazy next value or NoSuchElementException
     */
    public Future<V> oneOrErrorAsync(final K key, final Executor ex) {
        Future<V> res = Future.future();
        final CompletableFuture<Tuple0> cf = CompletableFuture.supplyAsync(() -> {

            final ValueSubscriber<V> sub = ValueSubscriber.subscriber();
            get(key).peek(a -> a.stream()
                    .subscribe(sub))
                    .map(a -> sub.toMaybe().visit(s -> {
                        res.complete(s);
                        return Tuple.empty();
                    }, () -> {
                        res.completeExceptionally(new NoSuchElementException());
                        return Tuple.empty();
                    }));
            return Tuple.empty();


        } , ex);

        return res;
    }

    /**
     * Return an Eval that allows retrieval of the next value from the attached pipe when get() is called,
     * can be used as an Iterator over the future & present values in the Adapter
     * 
     * Maybe.some is returned if a value is present, Maybe.none is returned if the publisher is complete or an error occurs
     * 
     * <pre>
     * {@code 
     *  Queue<String> q = new Queue<>();
        pipes.register("hello", q);
        pipes.push("hello", "world");
        pipes.push("hello", "world2");
        q.close();
        Eval<Maybe<String>> nextValue = pipes.nextValue("hello");
        int values = 0;
        while(nextValue.get().isPresent()){
            System.out.println(values++);
            
        }
            
        assertThat(values,equalTo(2));
     * 
     * 
     * }
     * </pre>
     * 
     * 
     * 
     * @param key : Adapter identifier
     * @return Eval that can lazily extract the next Value from the Adapter identified by the provided key once triggered
     */
    public Eval<Maybe<V>> nextValue(final K key) {
        final ValueSubscriber<V> sub = ValueSubscriber.subscriber();
        final LazyImmutable<Boolean> requested = LazyImmutable.def();
        final Option<Eval<Maybe<V>>> nested = get(key).peek(a -> a.stream()
                                                                 .subscribe(sub))
                                                     .map(a -> Eval.always(() -> {
                                                         if (requested.isSet()) {
                                                             sub.requestOne();
                                                         } else {
                                                             requested.setOnce(true);
                                                         }
                                                         final Maybe<V> res = sub.toMaybe();
                                                         return res;
                                                     }));
        return nested.orElse(Eval.now(Maybe.<V>nothing()));
    }

    /**
     * Return an Eval that allows retrieval of the next value from the attached pipe when get() is called
     * 
     * A value is returned if a value is present, otherwise null is returned if the publisher is complete or an error occurs
     * 
     * <pre>
     * {@code 
     *  Queue<String> q = new Queue<>();
        pipes.register("hello", q);
        pipes.push("hello", "world");
        pipes.push("hello", "world2");
        q.close();
        Eval<String> nextValue = pipes.nextOrNull("hello");
        int values = 0;
        while(nextValue.get()!=null){
            System.out.println(values++);
            
        }
            
        assertThat(values,equalTo(2));
     * 
     * 
     * }
     * </pre>
     * 
     * 
     * @param key : Adapter identifier
     * @return Eval that can lazily extract the next Value from the Adapter identified by the provided key once triggered
     */
    public Eval<V> nextOrNull(final K key) {
        final ValueSubscriber<V> sub = ValueSubscriber.subscriber();
        final LazyImmutable<Boolean> requested = LazyImmutable.def();
        return get(key).peek(a -> a.stream()
                                   .subscribe(sub))
                       .map(a -> Eval.always(() -> {
                           if (requested.isSet()) {
                               sub.requestOne();
                           } else {
                               requested.setOnce(true);
                           }
                           final Maybe<V> res = sub.toMaybe();

                           return res.orElse(null);
                       }))

                       .orElse(Eval.<V> now(null));
    }

    /**
     * Register a Queue, and get back a listening LazyFutureStream that runs on a singleUnsafe thread
     * (not the calling thread)
     * 
     * <pre>
     * {@code
     * Pipes.register("test", QueueFactories.
    										<String>boundedNonBlockingQueue(100)
    											.build());
    	LazyFutureStream<String> reactiveStream =  PipesToLazyStreams.cpuBoundStream("test");
    	reactiveStream.filter(it->it!=null).peek(System.out::println).run();
     * 
     * }</pre>
     * 
     * @param key : Adapter identifier
     * @param adapter
     * 
     */
    public void register(final K key, final Adapter<V> adapter) {
        registered.put(key, adapter);

    }

    /**
     * Clear all managed Adapters (without closing them or performing any other operation on them)
     */
    public void clear() {
        registered.clear();

    }

    /**
     * Subscribe synchronously to a pipe
     * 
     * @param key for registered simple-react async.Adapter
     * @param subscriber Reactive Streams reactiveSubscriber for data on this pipe
     */
    public void subscribeTo(final K key, final Subscriber<V> subscriber) {
        registered.get(key)
                  .stream()
                  .subscribe(subscriber);

    }

    /**
     *  Subscribe asynchronously to a pipe
     * 
     *  <pre>
     *  {@code 
     *  SeqSubscriber<String> reactiveSubscriber = SeqSubscriber.reactiveSubscriber();
        Queue<String> queue = new Queue();
        pipes.register("hello", queue);
        pipes.subscribeTo("hello",reactiveSubscriber,ForkJoinPool.commonPool());
        queue.offer("world");
        queue.close();
       
        assertThat(reactiveSubscriber.reactiveStream().findAny().get(),equalTo("world"));
     *  
     *  
     *  }
     *  </pre>
     * 
     * 
     * @param key for registered simple-react async.Adapter
     * @param subscriber Reactive Streams reactiveSubscriber for data on this pipe
     */
    public void subscribeTo(final K key, final Subscriber<V> subscriber, final Executor subscribeOn) {
        CompletableFuture.runAsync(() -> subscribeTo(key, subscriber), subscribeOn);

    }

    /**
     * Synchronously publish data to the Adapter specified by the provided Key, blocking the current thread
     * 
     * @param key for registered cylops-react async.Adapter
     * @param publisher Reactive Streams publisher  to push data onto this pipe
     */
    public void publishTo(final K key, final Publisher<V> publisher) {
        registered.get(key).fromStream(Spouts.from(publisher));
    }

    
    /**
     * Asynchronously publish data to the Adapter specified by the provided Key
     * 
     * <pre>
     * {@code 
     *  Pipes<String,Integer> pipes = Pipes.of();
        Queue<Integer> queue = new Queue();
        pipes.register("hello", queue);
        
        pipes.publishToAsync("hello",ReactiveSeq.of(1,2,3));
        
        Thread.sleep(100);
        queue.offer(4);
        queue.close();
       
        assertThat(queue.reactiveStream().toList(),equalTo(Arrays.asList(1,2,3,4)));
     * 
     * }
     * </pre>
     * 
     * 
     * @param key for registered simple-react async.Adapter
     * @param publisher Reactive Streams publisher  to push data onto this pipe
     */
    public void publishToAsync(final K key, final Publisher<V> publisher) {
        SequentialElasticPools.simpleReact.react(er -> er.of(publisher)
                                                         .peek(p -> publishTo(key, p)));
    }

    /**
     * Close the Adapter identified by the provided Key if it exists
     * 
     * @param key : Adapter identifier
     */
    public void close(final String key) {
        Optional.ofNullable(registered.get(key))
                .ifPresent(a -> a.close());

    }

}
