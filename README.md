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
            
;; define a function, normally this should be AOT-compiled but we can use a workaround
(defn hello! [x] (println "Hello" x "from Ignite!") :ok)        
```
Repeat the above in a different REPL (different JVM instance). 

In one of the REPLs:
```clj         
(require '[vermilionsands.ashtree.ignite :as ignite])  
(require '[vermilionsands.ashtree.compute :as compute])

;; broadcast a function to all nodes
(ignite/with-compute (ignite/compute ignite)
  (compute/broadcast 'user/hello! :args ["Stranger"])) 
```

You should see a printed string on both REPLs and a vector `[:ok :ok]` as a function
result on the calling one. 

#### Details

##### Sending a distributed task

To send a task to a single node use `invoke` function:

```clj
(ignite/with-compute compute-instance
  (compute/invoke some-foo))
  
;; would return a result from some-foo  
```

As a result you would get a return value from `some-foo`.

`ignite/with-compute` binds a compute API instance to `ignite/*compute*` which is later used by `invoke`. 
Alternatively you can use `invoke*` which accepts compute API instance as one of it's arguments.

```clj
(compute/invoke* compute-instance some-foo)
```

To pass arguments to a function use `:args args-vector` option. For no-arg functions you can
skip this, or pass `nil` or `[]`.

```clj 
(compute/invoke* compute-instance some-foo-with-args :args [arg1 arg2 arg3])
```

To send a task to all nodes use `broadcast` and `broadcast*`. It works in the same way as `invoke`
but returns a vector of results from all nodes.

```clj
(ignite/with-compute compute-instance
  (compute/broadcast some-foo))

;; would return a vector of results    
```

There's also `invoke-seq` and `invoke-seq*` which can be used to send multiple tasks at once. You can use it to mix
multiple different functions and call them together.

```clj 
(ignite/with-compute compute-instance 
  (compute/invoke-seq [some-foo1 some-foo2])
  (compite/invoke-seq [some-foo-with-args1 some-foo-with-arg2] :args [first-foo-args-vector second-foo-args-vector])
  
;; would return a vector of results   
``` 

##### Async

To enable async mode add `:async true` to task `:opts` like this:

```clj
(compute/invoke* compute-instance some-foo :opts {:async true})

;; would return IgniteFuture instance
```

Returned future is an instance of `IgniteFuture` which implements `IDeref` so it can be used in `deref` or `@`.

##### Tasks

Task function have to be available on all nodes that would be executing given task. Function can be either a no-arg
function (you can use `partial` to turn any function into a no-arg version) or can accept args which can be passed as 
one of the task options. 

Apart from standard functions there are two alternative ways to submit functions

* using a fully qualified symbol - internally this would be wrapped in a function that would try to resolve the symbol
  on executing node and call the function. 

  ```clj
  (ignite/with-compute (ignite/compute ignite)
    (compute/invoke 'some-ns/some-fn)) 
  ```  
  
* using serializable function from `vermilionsands.ashtree.function`. This is right now _experimental_. Serializable 
  functions would be passed in source form (along with local bindings) and evaled on execution to a concrete function.
  When working in REPL you can use this to submit arbitrary function to the cluster.
  
  ```clj
  (require '[vermilionsands.ashtree.function :as function])
  
  (ignite/with-compute (ignite/compute ignite)
    (compute/invoke (function/sfn [] (println "Hello from arbitrary function!")))) 
  ```
  
  There's a `*callable-eval*` var in `compute` ns which defines an `eval` function used to eval these functions.
  By default it would memoize 100 functions using LRU caching.      

##### Selecting nodes for execution
Which nodes would be eligible to process the task depends on cluster group used to get a compute instance. 

##### Task options

Task options can be passed in a map under `:opts` key like this:

```clj
(compute/invoke* compute-instance some-foo :opts {:async true :timeout 1000 :name "some-task-name"})
```

For all functions:
* `:async` - enable async execution if true
* `:timeout` - timeout, after which ComputeTaskTimeoutException would be returned, in milliseconds
* `:no-failover` - execute with no failover mode if true
* `:name` - name for this task

For `invoke`/`invoke*`
* `:affinity-cache` - cache name(s) for [affinity call](https://apacheignite.readme.io/docs/collocate-compute-and-data)
* `:affinity-key` - affinity key or partition id
 
For `invoke-seq`/`invoke-seq*`
* `:reduce` - if provided it would be called on the results reducing them into a single value.
It should accept 2 arguments (state, x) and should follow the same rules as task.
* `:reduce-init` - initial state of reducer state

##### misc
* in case of Binary Marshaller exceptions double check that you have your namespace with offending code AOTed. Usually
  it solves the problem. 
 
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
