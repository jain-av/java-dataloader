

        BatchLoader<Long, User> userBatchLoader = new BatchLoader<Long, User>() {
            @Override
            public CompletionStage<List<User>> load(List<Long> userIds) {
                return CompletableFuture.supplyAsync(() -> {
                    return userManager.loadUsersById(userIds);
                });
            }
        };

        DataLoader<Long, User> userLoader = DataLoaderFactory.newDataLoader(userBatchLoader);
```java
        BatchLoaderWithContext<String, String> batchLoader = new BatchLoaderWithContext<String, String>() {
            @Override
            public CompletionStage<List<String>> load(List<String> keys, BatchLoaderEnvironment environment) {
                SecurityCtx callCtx = environment.getContext();
                //
                // this is the load context objects in map form by key
                // in this case [ keyA : contextForA, keyB : contextForB ]
                //
                Map<Object, Object> keyContexts = environment.getKeyContexts();
                //
                // this is load context in list form
                //
                // in this case [ contextForA, contextForB ]
                return callDatabaseForResults(callCtx, keys);
            }
        };

        DataLoader<String, String> loader = DataLoaderFactory.newDataLoader(batchLoader, options);
        loader.load("keyA", "contextForA");
        loader.load("keyB", "contextForB");
```
```java
    DataLoaderFactory.newDataLoader(userBatchLoader, DataLoaderOptions.newOptions().setCachingEnabled(false));
``` 

Calling the above will ensure that every call to `.load()` will produce a new promise, and requested keys will not be saved in memory.
 
However, when the memoization cache is disabled, your batch function will receive an array of keys which may contain duplicates! Each key will 
be associated with each call to `.load()`. Your batch loader MUST provide a value for each instance of the requested key as per the contract

```java
        userDataLoader.load("A");
        userDataLoader.load("B");
        userDataLoader.load("A");

        userDataLoader.dispatch();

        // will result in keys to the batch loader with [ "A", "B", "A" ]

``` 

 
More complex cache behavior can be achieved by calling `.clear()` or `.clearAll()` rather than disabling the cache completely. 
 

## Caching errors
 
If a batch load fails (that is, a batch function returns a rejected CompletionStage), then the requested values will not be cached. 
However, if a batch function returns a `Try` or `Throwable` instance for an individual value, then that will be cached to avoid frequently loading 
the same problem object.
 
In some circumstances you may wish to clear the cache for these individual problems: 

```java
        userDataLoader.load("r2d2").whenComplete((user, throwable) -> {
            if (throwable != null) {
                userDataLoader.clear("r2dr");
                throwable.printStackTrace();
            } else {
                processUser(user);
            }
        });
```


## Statistics on what is happening

`DataLoader` keeps statistics on what is happening.  It can tell you the number of objects asked for, the cache hit number, the number of objects
asked for via batching and so on.

Knowing what the behaviour of your data is important for you to understand how efficient you are in serving the data via this pattern.


```java
        Statistics statistics = userDataLoader.getStatistics();
        
        System.out.println(format("load : %d", statistics.getLoadCount()));
        System.out.println(format("batch load: %d", statistics.getBatchLoadCount()));
        System.out.println(format("cache hit: %d", statistics.getCacheHitCount()));
        System.out.println(format("cache hit ratio: %d", statistics.getCacheHitRatio()));

```

`DataLoaderRegistry` can also roll up the statistics for all data loaders inside it.

You can configure the statistics collector used when you build the data loader

```java
        DataLoaderOptions options = DataLoaderOptions.newOptions().setStatisticsCollector(() -> new ThreadLocalStatisticsCollector());
        DataLoader<String,User> userDataLoader = DataLoaderFactory.newDataLoader(userBatchLoader,options);

```

Which collector you use is up to you.  It ships with the following: `SimpleStatisticsCollector`, `ThreadLocalStatisticsCollector`, `DelegatingStatisticsCollector` 
and `NoOpStatisticsCollector`.

## The scope of a data loader is important

If you are serving web requests then the data can be specific to the user requesting it.  If you have user specific data
then you will not want to cache data meant for user A to then later give it user B in a subsequent request.

The scope of your `DataLoader` instances is important.  You will want to create them per web request to ensure data is only cached within that
web request and no more.

If your data can be shared across web requests then use a custom `org.dataloader.ValueCache` to keep values in a common place.  

Data loaders are stateful components that contain promises (with context) that are likely share the same affinity as the request.

## Manual dispatching

The original [Facebook DataLoader](https://github.com/facebook/dataloader) was written in Javascript for NodeJS. 

NodeJS is single-threaded in nature, but simulates asynchronous logic by invoking functions on separate threads in an event loop, as explained
[in this post](http://stackoverflow.com/a/19823583/3455094) on StackOverflow.

NodeJS generates so-call 'ticks' in which queued functions are dispatched for execution, and Facebook `DataLoader` uses
the `nextTick()` function in NodeJS to _automatically_ dequeue load requests and send them to the batch execution function 
for processing.

Here there is an **IMPORTANT DIFFERENCE** compared to how `java-dataloader` operates!!

In NodeJS the batch preparation will not affect the asynchronous processing behaviour in any way. It will just prepare
batches in 'spare time' as it were.

This is different in Java as you will actually _delay_ the execution of your load requests, until the moment where you make a 
call to `dataLoader.dispatch()`.

Does this make Java `DataLoader` any less useful than the reference implementation? We would argue this is not the case,
and there are also gains to this different mode of operation:

- In contrast to the NodeJS implementation _you_ as developer are in full control of when batches are dispatched
- You can attach any logic that determines when a dispatch takes place
- You still retain all other features, full caching support and batching (e.g. to optimize message bus traffic, GraphQL query execution time, etc.)

However, with batch execution control comes responsibility! If you forget to make the call to `dispatch()` then the futures
in the load request queue will never be batched, and thus _will never complete_! So be careful when crafting your loader designs.

## The BatchLoader Scheduler

By default, when `dataLoader.dispatch()` is called, the `BatchLoader` / `MappedBatchLoader` function will be invoked
immediately.  

However, you can provide your own `BatchLoaderScheduler` that allows this call to be done some time into
the future.  

You will be passed a callback (`ScheduledBatchLoaderCall` / `ScheduledMapBatchLoaderCall`) and you are expected
to eventually call this callback method to make the batch loading happen.

The following is a `BatchLoaderScheduler` that waits 10 milliseconds before invoking the batch loading functions.

```java
        new BatchLoaderScheduler() {

            @Override
            public <K, V> CompletionStage<List<V>> scheduleBatchLoader(ScheduledBatchLoaderCall<V> scheduledCall, List<K> keys, BatchLoaderEnvironment environment) {
                return CompletableFuture.supplyAsync(() -> {
                    snooze(10);
                    return scheduledCall.invoke();
                }).thenCompose(Function.identity());
            }

            @Override
            public <K, V> CompletionStage<Map<K, V>> scheduleMappedBatchLoader(ScheduledMappedBatchLoaderCall<K, V> scheduledCall, List<K> keys, BatchLoaderEnvironment environment) {
                return CompletableFuture.supplyAsync(() -> {
                    snooze(10);
                    return scheduledCall.invoke();
                }).thenCompose(Function.identity());
            }

             @Override
             public <K> void scheduleBatchPublisher(ScheduledBatchPublisherCall scheduledCall, List<K> keys, BatchLoaderEnvironment environment) {
                 snooze(10);
                 scheduledCall.invoke();
             }
        };
```

You are given the keys to be loaded and an optional `BatchLoaderEnvironment` for informative purposes.  You can't change the list of 
keys that will be loaded via this mechanism say.

Also note, because there is a max batch size, it is possible for this scheduling to happen N times for a given `dispatch()`
call.  The total set of keys will be sliced into batches themselves and then the `BatchLoaderScheduler` will be called for
each batch of keys.  

Do not assume that a single call to `dispatch()` results in a single call to `BatchLoaderScheduler`.

This code is inspired from the scheduling code in the [reference JS implementation](https://github.com/graphql/dataloader#batch-scheduling)

## Scheduled Registry Dispatching

`ScheduledDataLoaderRegistry` is a registry that allows for dispatching to be done on a schedule. It contains a
predicate that is evaluated (per data loader contained within) when `dispatchAll` is invoked.

If that predicate is true, it will make a `dispatch` call on the data loader, otherwise is will schedule a task to
perform that check again. Once a predicate evaluated to true, it will not reschedule and another call to
`dispatchAll` is required to be made.

This allows you to do things like "dispatch ONLY if the queue depth is > 10 deep or more than 200 millis have passed
since it was last dispatched".

```java

        DispatchPredicate depthOrTimePredicate = DispatchPredicate
            .dispatchIfDepthGreaterThan(10)
            .or(DispatchPredicate.dispatchIfLongerThan(Duration.ofMillis(200)));

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
            .dispatchPredicate(depthOrTimePredicate)
            .schedule(Duration.ofMillis(10))
            .register("users",userDataLoader)
            .build();
```

The above acts as a kind of minimum batch depth, with a time overload. It won't dispatch if the loader depth is less
than or equal to 10 but if 200ms pass it will dispatch.

## Chaining DataLoader calls

It's natural to want to have chained `DataLoader` calls.

```java
        CompletableFuture<Object> chainedCalls = dataLoaderA.load("user1")
        .thenCompose(userAsKey -> dataLoaderB.load(userAsKey));
```

However, the challenge here is how to be efficient in batching terms.

This is discussed in detail in the  https://github.com/graphql-java/java-dataloader/issues/54 issue.

Since CompletableFuture's are async and can complete at some time in the future, when is the best time to call
`dispatch` again when a load call has completed to maximize batching?

The most naive approach is to immediately dispatch the second chained call as follows :

```java
        CompletableFuture<Object> chainedWithImmediateDispatch = dataLoaderA.load("user1")
                .thenCompose(userAsKey -> {
                    CompletableFuture<Object> loadB = dataLoaderB.load(userAsKey);
                    dataLoaderB.dispatch();
                    return loadB;
                });
```

The above will work however the window of batching together multiple calls to `dataLoaderB` will be very small and since
it will likely result in batch sizes of 1.

This is a very difficult problem to solve because you have to balance two competing design ideals which is to maximize the 
batching window of secondary calls in a small window of time so you customer requests don't take longer than necessary.
* If the batching window is wide you will maximize the number of keys presented to a `BatchLoader` but your request latency will increase.

* If the batching window is narrow you will reduce your request latency, but also you will reduce the number of keys presented to a `BatchLoader`.


### ScheduledDataLoaderRegistry ticker mode

The `ScheduledDataLoaderRegistry` offers one solution to this called "ticker mode" where it will continually reschedule secondary
`DataLoader` calls after the initial `dispatch()` call is made.

The batch window of time is controlled by the schedule duration setup at when the `ScheduledDataLoaderRegistry` is created.

```java
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
        .register("a", dataLoaderA)
        .register("b", dataLoaderB)
        .scheduledExecutorService(executorService)
        .schedule(Duration.ofMillis(10))
        .tickerMode(true) // ticker mode is on
        .build();

        CompletableFuture<Object> chainedCalls = dataLoaderA.load("user1")
        .thenCompose(userAsKey -> dataLoaderB.load(userAsKey));

```
When ticker mode is on the chained dataloader calls will complete but the batching window size will depend on how quickly
the first level of `DataLoader` calls returned compared to the `schedule` of the `ScheduledDataLoaderRegistry`.

If you use ticker mode, then you MUST `registry.close()` on the `ScheduledDataLoaderRegistry` at the end of the request (say) otherwise
it will continue to reschedule tasks to the `ScheduledExecutorService` associated with the registry.

You will want to look at sharing the `ScheduledExecutorService` in some way between requests when creating the `ScheduledDataLoaderRegistry`
otherwise you will be creating a thread per `ScheduledDataLoaderRegistry` instance created and with enough concurrent requests
you may create too many threads.

### ScheduledDataLoaderRegistry dispatching algorithm

When ticker mode is **false** the `ScheduledDataLoaderRegistry` algorithm is as follows :

* Nothing starts scheduled - some code must call `registry.dispatchAll()` a first time
* Then for every `DataLoader` in the registry
  * The `DispatchPredicate` is called to test if the data loader should be dispatched
    * if it returns **false** then a task is scheduled to re-evaluate this specific dataloader in the near future 
    * If it returns **true**, then `dataLoader.dispatch()` is called and the dataloader is not rescheduled again
* The re-evaluation tasks are run periodically according to the `registry.getScheduleDuration()`

When ticker mode is **true** the `ScheduledDataLoaderRegistry` algorithm is as follows:

* Nothing starts scheduled - some code must call `registry.dispatchAll()` a first time
* Then for every `DataLoader` in the registry
    * The `DispatchPredicate` is called to test if the data loader should be dispatched
        * if it returns **false** then a task is scheduled to re-evaluate this specific dataloader in the near future
        * If it returns **true**, then `dataLoader.dispatch()` is called **and** a task is scheduled to re-evaluate this specific dataloader in the near future
* The re-evaluation tasks are run periodically according to the `registry.getScheduleDuration()`

## Instrumenting the data loader code

A `DataLoader` can have a `DataLoaderInstrumentation` associated with it.  This callback interface is intended to provide
insight into working of the `DataLoader` such as how long it takes to run or to allow for logging of key events.

You set the `DataLoaderInstrumentation` into the `DataLoaderOptions` at build time.

```java


        DataLoaderInstrumentation timingInstrumentation = new DataLoaderInstrumentation() {
            @Override
            public DataLoaderInstrumentationContext<DispatchResult<?>> beginDispatch(DataLoader<?, ?> dataLoader) {
                long then = System.currentTimeMillis();
                return DataLoaderInstrumentationHelper.whenCompleted((result, err) -> {
                    long ms = System.currentTimeMillis() - then;
                    System.out.println(format("dispatch time: %d ms", ms));
                });
            }

            @Override
            public DataLoaderInstrumentationContext<List<?>> beginBatchLoader(DataLoader<?, ?> dataLoader, List<?> keys, BatchLoaderEnvironment environment) {
                long then = System.currentTimeMillis();
                return DataLoaderInstrumentationHelper.whenCompleted((result, err) -> {
                    long ms = System.currentTimeMillis() - then;
                    System.out.println(format("batch loader time: %d ms", ms));
                });
            }
        };
        DataLoaderOptions options = DataLoaderOptions.newOptions().setInstrumentation(timingInstrumentation);
        DataLoader<String, User> userDataLoader = DataLoaderFactory.newDataLoader(userBatchLoader, options);
        
```

The example shows how long the overall `DataLoader` dispatch takes or how long the batch loader takes to run.

### Instrumenting the DataLoaderRegistry

You can also associate a `DataLoaderInstrumentation` with a `DataLoaderRegistry`.  Every `DataLoader` registered will be changed so that the registry
`DataLoaderInstrumentation` is associated with it.  This allows you to set just the one `DataLoaderInstrumentation` in place and it applies to all 
data loaders.

```java
    DataLoader<String, User> userDataLoader = DataLoaderFactory.newDataLoader(userBatchLoader);
    DataLoader<String, User> teamsDataLoader = DataLoaderFactory.newDataLoader(teamsBatchLoader);
    
    DataLoaderRegistry registry = DataLoaderRegistry.newRegistry()
            .instrumentation(timingInstrumentation)
            .register("users", userDataLoader)
            .register("teams", teamsDataLoader)
            .build();
    
    DataLoader<String, User> changedUsersDataLoader = registry.getDataLoader("users");
```

The `timingInstrumentation` here will be associated with the `DataLoader` under the key  `users` and the key `teams`.  Note that since
DataLoader is immutable, a new changed object is created so you must use the registry to get the `DataLoader`.  


## Other information sources

- [Facebook DataLoader Github repo](https://github.com/facebook/dataloader)
- [Facebook DataLoader code walkthrough on YouTube](https://youtu.be/OQTnXNCDywA)
- [Using DataLoader and GraphQL to batch requests](http://gajus.com/blog/9/using-dataloader-to-batch-requests)

## Contributing

All your feedback and help to improve this project is very welcome. Please create issues for your bugs, ideas and
enhancement requests, or better yet, contribute directly by creating a PR.

When reporting an issue, please add a detailed instruction, and if possible a code snippet or test that can be used
as a reproducer of your problem.

When creating a pull request, please adhere to the current coding style where possible, and create tests with your
code so it keeps providing an excellent test coverage level. PR's without tests may not be accepted unless they only
deal with minor changes.

## Acknowledgements

This library was originally written for use within a [VertX world](http://vertx.io/) and it used the vertx-core `Future` classes to implement
itself.  All the heavy lifting has been done by this project : [vertx-dataloader](https://github.com/engagingspaces/vertx-dataloader)
including the extensive testing (which itself came from Facebook).

This particular port was done to reduce the dependency on Vertx and to write a pure Java 11 implementation with no dependencies and also
to use the more normative Java CompletableFuture.  

[vertx-core](http://vertx.io/docs/vertx-core/java/) is not a lightweight library by any means so having a pure Java 11 implementation is 
very desirable.


This library is entirely inspired by the great works of [Lee Byron](https://github.com/leebyron) and
[Nicholas Schrock](https://github.com/schrockn) from [Facebook](https://www.facebook.com/) whom we would like to thank, and
especially @leebyron for taking the time and effort to provide 100% coverage on the codebase. The original set of tests 
were also ported.



## Licensing

This project is licensed under the
[Apache Commons v2.0](https://www.apache.org/licenses/LICENSE-2.0) license.

Copyright &copy; 2016 Arnold Schrijver, 2017 Brad Baker and others
[contributors](https://github.com/graphql-java/java-dataloader/graphs/contributors)
