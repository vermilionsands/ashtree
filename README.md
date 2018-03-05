# ashtree

Playing with Apache Ignite in Clojure. 

## Features

Caveat emptor! This is work in progress.

### Distributed tasks

`vermilionsands.ashtree.compute` contains some helpers to work with Ignite's 
[compute grid & distributed closures](https://apacheignite.readme.io/docs/distributed-closures). 

Right now only a part of the API is implemented. 

#### Sample usage

Start Ignite in REPL:
```clj
;; Create ignite instance
(import '[org.apache.ignite Ignition Ignite IgniteCompute])
(import '[org.apache.ignite.cache CacheMode])
(import '[org.apache.ignite.configuration IgniteConfiguration AtomicConfiguration])

(def ignite 
  (Ignition/start
    (doto (IgniteConfiguration.)
      (.setPeerClassLoadingEnabled true)
      (.setAtomicConfiguration
      (doto (AtomicConfiguration.)
        (.setCacheMode CacheMode/REPLICATED))))))
            
;; define a function, normally this should be AOT-compiled but we can use a call with symbol as a workaround
(defn hello! [x] (println "Hello" x "from Ignite!") :ok)        
```
Repeat the above in a different REPL (different JVM instance). 

In one of the REPLs:
```clj         
(require '[vermilionsands.ashtree.ignite :as ignite])  
(require '[vermilionsands.ashtree.compute :as compute])

;; broadcast a function to all nodes
(compute/with-compute (ignite/compute ignite)
  (compute/invoke 'user/hello! :args ["Stranger"] :broadcast true)) 
```

You should see a printed string on both REPLs and a vector `[:ok :ok]` as a function
result on the calling one. 

#### Details

##### Sending a distributed task

To send a task to a single node use `invoke` function:

```clj
(compute/with-compute compute-instance
  (compute/invoke some-fn))
  
;; would return a result from some-fn  
```

As a result you would get a return value from `some-fn`.

`ignite/with-compute` binds a compute API instance to `compute/*compute*` which is later used by `invoke`. 
Alternatively you can use `:compute` option to submit a compute instance to invoke. 

```clj
(compute/invoke some-fn :compute compute-instance)
```

To pass arguments to a function use `:args args-vector` option. For no-arg functions you can
skip this, or pass `nil` or `[]`.

```clj 
(compute/with-compute compute-instance 
  (compute/invoke some-fn-with-args :args [arg1 arg2 arg3]))
```

##### Broadcast

To send a task to all nodes use `:broadcast` option. Tasks would be executed on each node in cluster group associated
with compute instance and it would return a vector of results from all nodes.

```clj
(compute/with-compute compute-instance
  (compute/invoke some-fn :broadcast true))

;; would return a vector of results    
```

##### Call many tasks in one call

There's also an option to to send multiple tasks at once. You can use it to mix multiple different functions 
and call them together.

```clj 
(compute/with-compute compute-instance 
  (compute/invoke some-fn-with-args1 some-fn-with-arg2 :args [first-fn-args-vector] [second-fn-args-vector])
  
;; would return a vector of results   
``` 

##### Async

To enable async mode add `:async true` option like this:

```clj
(compute/with-compute compute-instance
  (compute/invoke compute-instance some-fn :async true)

;; would return an IgniteFuture instance
```

Returned future is an instance of `IgniteFuture` which implements `IDeref`, `IPending` and `IBlockingDeref` so it 
can be used in `deref` or `@` like standard `future`.

There is a `finvoke` function which is a variant of `invoke` that adds `:async true` option.

##### Tasks

Task function have to be available on all nodes that would be executing given task. Function can be either a no-arg
function (you can use `partial` to turn any function into a no-arg version) or can accept args which can be passed as 
one of the task options. Right now it has to be an instance of `AFn` so some classes that implement `IFn` interface 
would not qualify (this may change in future). 

Apart from standard functions there are two alternative ways to submit functions

* using a fully qualified symbol - internally this would be wrapped in a function that would try to resolve the symbol
  on executing node and call the function. 

  ```clj
  (compute/with-compute compute-instance
    (compute/invoke 'some-ns/some-fn)) 
  ```  
  
* using serializable function from `vermilionsands.ashtree.function`. This is right now _experimental_. Serializable 
  functions would be passed in source form (along with local bindings) and evaled on execution to a concrete function.
  When working in REPL you can use this to submit arbitrary function to the cluster.
  
  ```clj
  (require '[vermilionsands.ashtree.function :as function])
  
  (compute/with-compute compute-instance
    (compute/invoke (function/sfn [] (println "Hello from arbitrary function!")))) 
  ```
  
  There's a `*callable-eval*` var in `function` ns which defines an `eval` function used to eval these functions.
  By default it would memoize 100 functions using LRU caching.      

##### Selecting nodes for execution
Which nodes would be eligible to process the task depends on cluster group used to get a compute instance. 

##### Task options

`invoke` supports the following options, which can be supplied as additional arguments:

```clj
(compute/invoke some-fn :args [1 2] :async true :name "distributed-function" :timeout 1000)
```

* `:args` - seqs of arguments for tasks
* `:async` - enable async execution if true
* `:broadcast` - would be executed on all nodes if true
* `:compute` - compute API instance, would override `*compute*`
* `:name` - name for this task
* `:no-failover` - execute with no failover mode if true
* `:timeout` - timeout, after which ComputeTaskTimeoutException would be returned, in milliseconds

For `invoke` with single task:
* `:affinity-cache` - cache name(s) for [affinity call](https://apacheignite.readme.io/docs/collocate-compute-and-data)
* `:affinity-key` - affinity key or partition id
 
For `invoke` with multiple tasks:
* `:reduce` - if provided it would be called on the results reducing them into a single value.
It should accept 2 arguments (state, x) and should follow the same rules as task.
* `:reduce-init` - initial state of reducer state

See `invoke` doc for more info. If instead of additional args you would like to pass options as map check `invoke*`.

##### Distributed function 

`distribute` function accepts a task and additional options and returns a new function which would
call `invoke` with supplied task, options and args. If it is called with bounded `*compute*` or with `:compute` 
option, new function would keep compute API instance.

```clj
;; using hello! from earlier example
;; create a distributed version
(def distributed-hello! 
  (compute/invoke 'user/hello! :compute (ignite/compute ignite)))
  
;; call it later, it would be executed on cluster
(distributed-hello! "John")  
```

There is a `fdistribute` function which is a variant of `distribute` that adds `:async true` option.

##### misc
* in case of Binary Marshaller exceptions double check that you have your namespace with offending code AOT compiled. 
  Usually it solves the problem. 
 
### Distributed atom

`vermilionsands.ashtree.data` contains an atom-like reference type, that uses Ignite to store it's state in a 
distributed fashion.

#### Sample usage

In REPL:

```clj
;; Create Ignite instance first, see example for distributed task for example.
;; After that:

;; Create atom
(require '[vermilionsands.ashtree.data :as data])
(def ignite-atom (data/distributed-atom ignite :test-atom 0))
```

Repeat the above steps in a different REPL (different JVM instance). 
After that you can use distributed atom like a regular one.

Atom value after calls to `swap!` and `reset!` in one JVM would be available in another.

In one REPL:
```clj
;; set value in one REPL
(reset! ignite-atom "my shared value")
```

After that in another one:
```clj
;; check that value is visible
@ignite-atom ;; => would return "my shared value"
```

#### Details

`vermilionsands.ashtree.data` defines an `IgniteAtom` type which implements the same interfaces as `clojure.lang.Atom`, 
as well as a constructor function `distributed-atom`.
 
##### swap! and reset!

`swap!` is implemented using `compareAndSet` and keeps data in 
[IgniteAtomicReference](https://apacheignite.readme.io/docs/atomic-types). Values need to be serializable.
Functions (for `swap!`) are executed locally and do not need to be serializable. To use custom deftypes as values make 
their namespaces AOTed.  

##### validators

Validator added using `set-validator!` is not shared between instances. Shared validator can be added using 
`set-shared-validator` from `vermilionsands.ashtree.data/DistributedAtom` protocol. This functions would be 
shared between instances.
  
**Note:** validator function should be AOTed on all JVM instances and has to be serializable.
    
Validation is called only on the instance which is changing the data.
 
##### watches
 
Watches added using `add-watch` are local (like validator). Shared watches can be added using `add-shared-watch` 
from `DistributedAtom` protocol are shared between instances. Functions need to meet the same requirements as shared 
validators.
  
By default notification that triggers watches is executed only on the instance which is changing the data (like with 
validation). 
There is an option to enable global notifications, which would trigger watches on all instances.
 
In order to turn on global notifications add `:global-notifications true` in options map to `distributed-atom` function when first 
atom instance is created.
This would enable notifications that would be used to trigger watches when a change occurs.
Notifications can have a timeout, set using `:notification-timeout milliseconds` option.

Notifications are implemented using `IgniteMessaging`. Each notification is a vector od `[old-value new-value]`.  
  
##### metadata

Metadata is stored on instance and is not shared.

##### skipping identity updates
`:skip-identity true` option would make the atom skip updating it's state if `(= new-val old-val)`. No notifications 
would be called as well. This can improve performance if you have lot's of such updates, but use this option at your own 
discretion. 

##### misc
  
* partitioning and replication are controlled by Ignite instance
* to free resources when no longer needed call `(destroy! distributed-atom)`. This would destroy all underlying 
  distributed objects
* when notifications are enabled atom needs to be closed using `.close()` from `java.io.Closeable`. 
  Otherwise notification listener would not be removed. This would remove the listener, but would not destroy 
  state objects. `destroy!` would do both.
  
 
## License

Copyright © 2018 vermilionsands

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
