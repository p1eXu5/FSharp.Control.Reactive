namespace p1eXu5.FSharp.Reactive

open System
open System.Collections.Generic
open System.Reactive
open System.Reactive.Concurrency
open System.Reactive.Linq
open System.Threading
open System.Threading.Tasks

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    // ------------------------
    // Create
    // ------------------------

    /// Connects the observable wrapper to its source. All subscribed
    /// observers will recieve values from the underlying observable
    /// sequence as long as the connection is established.
    /// ( publish an Observable to get a ConnectableObservable )
    let inline connect ( source:Subjects.IConnectableObservable<_> ) =
        source.Connect()

    /// Creates an observable sequence from the specified Subscribe method implementation.
    let create (subscribe: IObserver<'a> -> unit) =
        let subscribe' o =
            let m = subscribe o
            Action(fun () -> m)
        Observable.Create(subscribe')

    /// Creates an observable that calls the specified function (each time)
    /// after an observer is attached to the observable. This is useful to
    /// make sure that events triggered by the function are handled.
    let inline createTee f (source: IObservable<'Args>) =
        Observable.Create (fun observer ->
            let disposable = source.Subscribe observer in f ()
            disposable
        )

    /// Creates an observable sequence from the specified asynchronous Subscribe method implementation.
    let inline createAsync (subscribe: IObserver<'a> -> Async<IDisposable>) =
        Observable.Create(subscribe >> Async.StartAsTask)

    /// Creates an observable sequence from the specified asynchronous Subscribe method implementation.
    let inline createTask (subscribe: IObserver<'a> -> Task<IDisposable>) =
        Observable.Create(subscribe)

        /// Creates an observable sequence from the specified asynchronous Subscribe method implementation.
    let inline createTaskCt (subscribe: IObserver<'a> -> CancellationToken -> Task<IDisposable>) =
        Observable.Create(subscribe)

    /// Returns an observable which emits a single value
    let inline retn x : IObservable<_> = Observable.Return x

    ///  Returns an observable sequence that contains a single element,
    /// using a specified scheduler to send out observer messages.
    let inline retnOn (scheduler: IScheduler) value =
        Observable.Return(value, scheduler)

    /// Returns an empty observable
    let inline empty<'a> () = Observable.Empty<'a>()

    /// Returns an empty sequence, using the specified scheduler to send out the single OnCompleted message.
    let inline emptyOn (scheduler: IScheduler) =
        Observable.Empty(scheduler)

    /// Returns an empty Observable sequence.
    ///
    /// The witness acts as a placeholder value (often default(T) or null) to help the compiler deduce the type parameter.
    let inline emptyOf<'a>(witness:'a) :IObservable<'a> =
        Observable.Empty(witness)

    /// Returns an empty sequence, using the specified scheduler to send out the single OnCompleted message.
    ///
    /// The witness acts as a placeholder value (often default(T) or null) to help the compiler deduce the type parameter.
    let inline emptySchOf (scheduler: IScheduler) witness =
        Observable.Empty(scheduler, witness)

    /// The Observable.Never<T>() method returns a sequence which, like Empty, does not produce any values,
    /// but unlike Empty, it never ends. 
    let inline never () =
        Observable.Never()

    /// The Observable.Never<T>() method returns a sequence which, like Empty, does not produce any values,
    /// but unlike Empty, it never ends.
    ///
    /// The witness acts as a placeholder value (often default(T) or null) to help the compiler deduce the type parameter.
    let inline neverWitness<'a> (witness: 'a) =
        Observable.Never(witness)


    // ------------------------
    // Start
    // ------------------------

    /// Allows you to turn a long running Func<T> or Action into a single value
    /// observable sequence.
    ///
    /// The action is invoked through a scheduler. If you don’t pass a scheduler explicitly,
    /// this will use the DefaultScheduler, which invokes the callback via the thread pool. 
    ///
    /// When the function returns its value, the
    /// IObservable<T>, will supply that value to subscribers and then complete immediately after supplying the
    /// value.
    let inline startUnit f =
        Observable.Start(Action f)

    /// Allows you to turn a long running Func<T> or Action into a single value
    /// observable sequence.
    ///
    /// The action is invoked through a scheduler. If you don’t pass a scheduler explicitly,
    /// this will use the DefaultScheduler, which invokes the callback via the thread pool. 
    ///
    /// When the function returns its value, the
    /// IObservable<T>, will supply that value to subscribers and then complete immediately after supplying the
    /// value.
    let inline startUnitOn (scheduler: IScheduler) f =
        Observable.Start(Action f, scheduler)

    /// Allows you to turn a long running Func<T> or Action into a single value
    /// observable sequence.
    ///
    /// The action is invoked through a scheduler. If you don’t pass a scheduler explicitly,
    /// this will use the DefaultScheduler, which invokes the callback via the thread pool. 
    ///
    /// When the function returns its value, the
    /// IObservable<T>, will supply that value to subscribers and then complete immediately after supplying the
    /// value.
    let inline start<'a> (f: unit -> 'a) =
        Observable.Start(Func<'a> f)

    /// Allows you to turn a long running Func<T> or Action into a single value
    /// observable sequence.
    ///
    /// The action is invoked through a scheduler. If you don’t pass a scheduler explicitly,
    /// this will use the DefaultScheduler, which invokes the callback via the thread pool. 
    ///
    /// When the function returns its value, the
    /// IObservable<T>, will supply that value to subscribers and then complete immediately after supplying the
    /// value.
    let inline startOn<'a> (scheduler: IScheduler) (f: unit -> 'a) =
        Observable.Start(Func<'a> f, scheduler)

    // ------------------------
    // Subscribe
    // ------------------------

    let inline subscribeNext (onNext: _ -> unit) (observable: #IObservable<_>) =
        observable.Subscribe(Action<_> onNext)

    let inline subscribeCompleted (onCompleted: unit -> unit) (observable: #IObservable<_>) =
        observable.Subscribe(Action<_> ignore, Action onCompleted)

    /// Subscribes to the Observable with a next and an error-function.
    let inline subscribeNextError (onNext: _ -> unit) (onError: exn -> unit) (observable: #IObservable<_>) =
        observable.Subscribe(Action<_> onNext, Action<exn> onError)

    /// Subscribes to the Observable with a next and a completion callback.
    let inline subscribeNextCompleted (onNext: _ -> unit) (onCompleted: unit -> unit) (observable: #IObservable<_>) =
        observable.Subscribe(Action<_> onNext, Action onCompleted)

    /// Subscribes to the Observable with a next and a completion callback.
    let inline subscribeErrorCompleted (onError: exn -> unit) (onCompleted: unit -> unit) (observable: #IObservable<_>) =
        observable.Subscribe(Action<_> ignore, Action<exn> onError, Action onCompleted)

    /// Subscribes to the Observable with a next function.
    let inline subscribe (onNext: _ -> unit) (observable: #IObservable<_>) =
        observable.Subscribe(Action<_> onNext)

    /// Subscribes to the Observable with a next and an error-function.
    let inline subscribeWithError (onNext: _ -> unit) (onError: exn -> unit) (observable: #IObservable<_>) =
        observable.Subscribe(Action<_> onNext, Action<exn> onError)

    /// Subscribes to the Observable with a next and a completion callback.
    let inline subscribeWithCompletion (onNext: _ -> unit) (onCompleted: unit -> unit) (observable: #IObservable<_>) =
            observable.Subscribe(Action<_> onNext, Action onCompleted)

    /// Subscribes to the observable with all three callbacks
    let inline subscribeWithCallbacks (onNext: _ -> unit) (onError: exn -> unit) (onCompleted: unit -> unit) (observable: #IObservable<_>) =
        observable.Subscribe(Observer.Create(Action<_> onNext, Action<_> onError, Action onCompleted))

    /// Subscribes to the observable with the given observer
    let inline subscribeObserver (observer: #IObserver<_>) (observable: #IObservable<_>) =
        observable.Subscribe(observer)

    /// Wraps the source sequence in order to run its subscription and unsubscription logic
    /// on the specified scheduler. This operation is not commonly used;  This only performs
    /// the side-effects of subscription and unsubscription on the specified scheduler.
    ///  In order to invoke observer callbacks on a scheduler, use 'observeOn'
    let inline subscribeOn (scheduler: Reactive.Concurrency.IScheduler) (source: #IObservable<_>) : IObservable<_> =
        Observable.SubscribeOn(source, scheduler)

    /// Wraps the source sequence in order to run its subscription and unsubscription logic
    /// on the specified SynchronizationContext. This operation is not commonly used;  This only performs
    /// the side-effects of subscription and unsubscription on the specified scheduler.
    ///  In order to invoke observer callbacks on a scheduler, use 'observeOn'
    let inline subscribeOnContext (context:Threading.SynchronizationContext) (source:IObservable<'Source>) : IObservable<'Source> =
        Observable.SubscribeOn( source, context )


    /// Subscribes to the specified source, re-routing synchronous exceptions during invocation of the
    /// Subscribe function to the observer's 'OnError channel. This function is typically used to write query operators.
    let inline subscribeSafe onNext (source : IObservable<_>) =
        source.SubscribeSafe (Observer.Create (Action<_> onNext, Action<_> ignore, Action ignore))


    /// Subscribes to the specified source, re-routing synchronous exceptions during invocation of the
    /// Subscribe function to the observer's 'OnError channel. This function is typically used to write query operators.
    let inline subscribeSafeWithError onNext onError (source : IObservable<_>) =
        source.SubscribeSafe (Observer.Create (Action<_> onNext, Action<_> onError, Action ignore))


    /// Subscribes to the specified source, re-routing synchronous exceptions during invocation of the
    /// Subscribe function to the observer's 'OnError channel. This function is typically used to write query operators.
    let inline subscribeSafeWithCompletion onNext onCompleted (source : IObservable<_>) =
        source.SubscribeSafe (Observer.Create (Action<_> onNext, Action<_> ignore, Action ignore))


    /// Subscribes to the specified source, re-routing synchronous exceptions during invocation of the
    /// Subscribe function to the observer's 'OnError channel. This function is typically used to write query operators.
    let inline subscribeSafeObserver observer (source : IObservable<_>) =
        source.SubscribeSafe observer


    /// Subscribes to the specified source, re-routing synchronous exceptions during invocation of the
    /// Subscribe function to the observer's 'OnError channel. This function is typically used to write query operators.
    let inline subscribeSafeWithCallbacks onNext onError onCompleted (source : IObservable<_>) =
        source.SubscribeSafe (Observer.Create (Action<_> onNext, Action<_> onError, Action onCompleted))



    // ------------------------
    // Other
    // ------------------------

    /// Returns an observable sequence that terminates with an exception.
    let inline throw<'a> (ex: exn) : IObservable<'a> =
        Observable.Throw(ex)

    /// Returns an observable sequence that terminates with an exception.
    let inline throwOf<'a> (witness: 'a) (ex: exn) : IObservable<'a> =
        Observable.Throw(ex, witness=witness)

    /// Returns an observable sequence that terminates with an exception,
    /// using the specified scheduler to send out the single OnError message.
    let inline throwSch<'a> (scheduler: IScheduler) (ex: exn) : IObservable<'a> =
        Observable.Throw(ex, scheduler=scheduler)

    /// Returns an observable sequence that terminates with an exception,
    /// using the specified scheduler to send out the single OnError message.
    let inline throwSchOf<'a> (scheduler: IScheduler) (witness: 'a) (ex: exn) =
        Observable.Throw(ex, scheduler, witness)

    /// Returns an observable sequence that invokes the specified factory function whenever a new observer subscribes.
    let inline defer<'a> (observableFactory: unit -> IObservable<'a>): IObservable<'a> =
        Observable.Defer(Func<IObservable<'a>> observableFactory)


    // ------------------------
    // From...
    // ------------------------

    /// Converts an Action-based .NET event to an observable sequence. Each event invocation is surfaced through an OnNext message in the resulting sequence.
    /// For conversion of events conforming to the standard .NET event pattern, use any of the FromEventPattern overloads instead.
    let inline fromEvent ( addHandler )( removeHandler ) : IObservable<unit> =
        Observable.FromEvent( Action<'Delegate> addHandler, Action<'Delegate> removeHandler )

    /// Converts an Action-based .NET event to an observable sequence. Each event invocation is surfaced through an OnNext message in the resulting sequence.
    /// For conversion of events conforming to the standard .NET event pattern, use any of the FromEventPattern overloads instead.
    let fromEventOn (scheduler: IScheduler) addHandler removeHandler =
        Observable.FromEvent(Action<_> addHandler, Action<_> removeHandler, scheduler)

    /// Converts an generic Action-based .NET event to an observable sequence. Each event invocation is surfaced through an OnNext message in the resulting sequence.
    /// For conversion of events conforming to the standard .NET event pattern, use any of the FromEventPattern overloads instead.
    let inline fromEventGeneric addHandler removeHandler : IObservable<'TEventArgs> =
        Observable.FromEvent(Action<'TEventArgs -> unit> addHandler, Action<'TEventArgs -> unit> removeHandler)

    /// Converts an generic Action-based .NET event to an observable sequence. Each event invocation is surfaced through an OnNext message in the resulting sequence.
    /// For conversion of events conforming to the standard .NET event pattern, use any of the FromEventPattern overloads instead.
    let inline fromEventGenericOn (scheduler: IScheduler) addHandler removeHandler =
        Observable.FromEvent(Action<#EventArgs -> unit> addHandler, Action<#EventArgs -> unit> removeHandler, scheduler)

    /// Converts a .NET event to an observable sequence, using a conversion function to obtain the event delegate.
    /// Each event invocation is surfaced through an OnNext message in the resulting sequence.
    /// For conversion of events conforming to the standard .NET event pattern, use any of the FromEventPattern functions instead.
    let inline fromEventConversion conversion addHandler removeHandler =
        Observable.FromEvent(
            conversion = Func<Action<#EventArgs>, unit> (fun action -> conversion (fun args -> action.Invoke(args))),
            addHandler = Action<_> addHandler,
            removeHandler = Action<_> removeHandler
        )

    /// Converts a .NET event to an observable sequence, using a conversion function to obtain the event delegate, using a specified scheduler to run timers.
    /// Each event invocation is surfaced through an OnNext message in the resulting sequence.
    /// For conversion of events conforming to the standard .NET event pattern, use any of the FromEventPattern functions instead.
    let inline fromEventConversionOn (scheduler: IScheduler) conversion addHandler removeHandler =
        Observable.FromEventPattern
            (Func<EventHandler<'TEventArgs>, 'TDelegate> conversion, Action<'TDelegate> addHandler, Action<'TDelegate> removeHandler, scheduler)

    /// Converts a .NET event to an observable sequence, using a supplied event delegate type.
    /// Each event invocation is surfaced through an OnNext message in the resulting sequence.
    let inline fromEventHandler addHandler removeHandler =
        Observable.FromEventPattern<#EventArgs> (
                    Action<EventHandler<_>> addHandler,
                    Action<EventHandler<_>> removeHandler)


    /// Converts a .NET event to an observable sequence, using a supplied event delegate type on a specified scheduler.
    /// Each event invocation is surfaced through an OnNext message in the resulting sequence.
    let inline fromEventHandlerOn (scheduler: IScheduler) addHandler removeHandler =
        Observable.FromEventPattern<#EventArgs> (
            Action<EventHandler<_>> addHandler,
            Action<EventHandler<_>> removeHandler,
            scheduler)


    /// Generates an observable from an IEvent<_> as an EventPattern.
    let inline fromEventPattern eventName (target:obj) =
        Observable.FromEventPattern( target, eventName )

    /// Observable.FromAsync
    let inline fromUnitTask task =
        Observable.FromAsync(
            Func<CancellationToken, Task> task
        )

    /// Observable.FromAsync
    let inline fromTask task =
        Observable.FromAsync(
            Func<CancellationToken, Task<'a>> task
        )

    /// Turns an F# async workflow into an observable
    let inline fromAsync asyncOperation =
        Observable.FromAsync(
            fun (token : Threading.CancellationToken) -> Async.StartAsTask(asyncOperation, cancellationToken = token)
        )

    /// Helper function for turning async workflows into observables
    let inline liftAsync asyncOperationf =
        asyncOperationf >> fromAsync

    /// Returns the sequence as an observable
    let inline ofSeq<'Item>(source:'Item seq) : IObservable<'Item> =
        Observable.ToObservable source

    /// Returns the sequence as an observable, using the specified scheduler to run the enumeration loop
    let inline ofSeqOn<'Item> (scheduler: Concurrency.IScheduler) (items:'Item seq) : IObservable<'Item> =
        Observable.ToObservable (items, scheduler)

    /// Hides the identy of an observable sequence
    let inline asObservable source : IObservable<'Source>=
        Observable.AsObservable( source )

    /// Observable.ForEachAsync(source, Action<'Source> map)
    let inline foreachTask map (source:IObservable<'Source>) =
        Observable.ForEachAsync(source, Action<'Source> map)

    /// Observable.ForEachAsync(source, Action<'Source> map, ct)
    let inline foreachTaskCt map (ct: Threading.CancellationToken) (source:IObservable<'Source>) =
        Observable.ForEachAsync(source, Action<'Source> map, ct)

    /// Observable.ForEachAsync(source, Action<'Source, int> map)
    let inline foreachTaski mapi (source:IObservable<'Source>) =
        Observable.ForEachAsync(source, Action<'Source, int> mapi)

    /// Observable.ForEachAsync(source, Action<'Source, int> map, ct)
    let inline foreachTaskCti mapi (ct: Threading.CancellationToken) (source:IObservable<'Source>) =
        Observable.ForEachAsync(source, Action<'Source, int> mapi, ct)

    /// Projects each element of an observable sequence to a task by incorporating the element's index
    /// and merges all of the task results into one observable sequence.
    //    let flatmapTaski  ( map ) ( source:IObservable<'Source> ) : IObservable<'Result> =
    //        Observable.SelectMany( source, Func<'Source,int,Threading.Tasks.Task<'Result>> map )


    /// Returns an enumerator that enumerates all values of the observable sequence.
    let inline getEnumerator ( source ) : IEnumerator<_> =
        Observable.GetEnumerator( source )

    /// Invokes an action for each element in the observable sequence, and propagates all observer
    /// messages through the result sequence. This method can be used for debugging, logging, etc. of query
    /// behavior by intercepting the message stream to run arbitrary actions for messages on the pipeline.
    let inline iter ( onNext ) ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.Do( source, Action<'Source> onNext )

    /// Invokes an action for each element in the observable sequence and invokes an action
    /// upon graceful termination of the observable sequence. This method can be used for debugging,
    ///  logging, etc. of query behavior by intercepting the message stream to run arbitrary
    /// actions for messages on the pipeline.
    let inline iterEnd ( onNext )( onCompleted ) ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.Do( source, Action<'Source> onNext, Action onCompleted )


    /// Invokes an action for each element in the observable sequence and invokes an action upon
    /// exceptional termination of the observable sequence. This method can be used for debugging,
    /// logging, etc. of query behavior by intercepting the message stream to run arbitrary
    /// actions for messages on the pipeline.
    let inline iterError ( onNext)( onError ) ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.Do( source, Action<'Source> onNext, Action<exn> onError )


    /// Invokes an action for each element in the observable sequence and invokes an action
    /// upon graceful or exceptional termination of the observable sequence.
    /// This method can be used for debugging, logging, etc. of query behavior by intercepting
    /// the message stream to run arbitrary actions for messages on the pipeline.
    let inline iterErrorEnd ( onNext )( onError ) ( onCompleted ) ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.Do( source, Action<'Source> onNext, Action<exn> onError, Action onCompleted )


    /// Invokes the observer's methods for each message in the source sequence.
    /// This method can be used for debugging, logging, etc. of query behavior by intercepting
    /// the message stream to run arbitrary actions for messages on the pipeline.
    let inline iterObserver ( observer:IObserver<'Source> ) ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.Do( source,observer )

        
    /// Returns an enumerable sequence whose enumeration returns the latest observed element in the source observable sequence.
    /// Enumerators on the resulting sequence will never produce the same element repeatedly,
    /// and will block until the next element becomes available.
    let inline latest source =
        Observable.Latest( source )

        
    /// Produces an enumerable sequence of consequtive (possibly empty) chunks of the source observable
    let inline chunkify<'Source> source : seq<IList<'Source>> =
        Observable.Chunkify<'Source>( source )

    /// Returns an enumerable sequence whose sequence whose enumeration returns the
    /// most recently observed element in the source observable sequence, using
    /// the specified
    let inline mostRecent initialVal source =
        Observable.MostRecent( source, initialVal )

    

    /// Returns an observable sequence whose enumeration blocks until the next
    /// element in the source observable sequence becomes available.
    /// Enumerators  on the resulting sequence will block until the next
    /// element becomes available.
    let inline next source =
        Observable.Next( source )

    /// Wraps the source sequence in order to run its observer callbacks on the specified scheduler.
    let inline observeOn (scheduler:Concurrency.IScheduler) source =
        Observable.ObserveOn( source, scheduler )

    /// Wraps the source sequence in order to run its observer callbacks
    /// on the specified synchronization context.
    let inline observeOnContext (context:SynchronizationContext) source =
        Observable.ObserveOn( source, context )

    /// Iterates through the observable and performs the given side-effect
    let inline perform f source =
        let inline inner x = f x
        Observable.Do(source, inner)

    /// Logs the incoming emits with a given prefix to a specified target.
    let logTo prefix f source =
        let onNext = Action<_> (fun x -> f (sprintf "%s - OnNext(%A)" prefix x))
        let onError = Action<exn> (fun ex ->
            f (sprintf "%s - OnError:" prefix)
            f (sprintf "\t %A" ex))
        let onCompleted = Action (fun () -> f (sprintf "%s - OnCompleted()" prefix))
        Observable.Do(source, onNext, onError, onCompleted)

    /// Logs the incoming emits with a given prefix to the console.
    let inline log prefix source =
        logTo prefix Console.WriteLine source

    /// Invokes the finally action after source observable sequence terminates normally or by an exception.
    let inline performFinally f source = Observable.Finally(source, Action f)

    /// Repeats the given observable sequence as long as the specified condition holds, where the
    /// condition is evaluated after each repeated source is completed.
    let inline repeatWhile ( condition)( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.DoWhile( source, Func<bool> condition)

    /// If the condition evaluates true, select the "thenSource" sequence. Otherwise, return an empty sequence.
    let inline selectIf condition thenSource =
        Observable.If( Func<bool> condition, thenSource )

    /// If the condition evaluates true, select the "thenSource" sequence.
    /// Otherwise, return an empty sequence generated on the specified scheduler.
    let inline selectIfOn (scheduler:IScheduler) condition thenSource =
        Observable.If( Func<bool> condition, thenSource, scheduler)

    /// If the condition evaluates true, select the "thenSource" sequence. Otherwise, select the else source
    let inline selectIfElse condition ( elseSource : IObservable<'Result>)
                                ( thenSource : IObservable<'Result>) =
        Observable.If( Func<bool> condition, thenSource, elseSource )

    /// Synchronizes the observable sequence so that notifications cannot be delivered concurrently
    /// this overload is useful to "fix" an observable sequence that exhibits concurrent
    /// callbacks on individual observers, which is invalid behavior for the query processor
    let inline synchronize source : IObservable<_>=
        Observable.Synchronize(source)

    /// Synchronizes the observable sequence such that observer notifications
    /// cannot be delivered concurrently, using the specified gate object.This
    /// overload is useful when writing n-ary query operators, in order to prevent
    /// concurrent callbacks from different sources by synchronizing on a common gate object.
    let inline synchronizeGate (gate:obj)  (source:IObservable<_>): IObservable<_> =
        Observable.Synchronize(source, gate)

    /// Converts an observable into a seq
    let inline toEnumerable (source: IObservable<'a>) = Observable.ToEnumerable(source)
    /// Creates an array from an observable sequence

    /// Creates an array from an observable sequence.
    let inline toArray  source =
        Observable.ToArray(source)

    /// Creates an observable sequence according to a specified key selector function
    let inline toDictionary keySelector source =
        Observable.ToDictionary(source, Func<_,_> keySelector)

    /// Creates an observable sequence according to a specified key selector function
    /// and an a comparer
    let inline toDictionaryComparer (keySelector:'Source->'Key) (comparer:'Key) (source:'Source) =
        Observable.ToDictionary( source, keySelector, comparer )

    /// Creates an observable sequence according to a specified key selector function
    let inline toDictionaryElements (keySelector:'Source->'Key )(elementSelector:'Source->'Elm) (source:'Source) =
        Observable.ToDictionary(source, keySelector, elementSelector)

    /// Creates an observable sequence according to a specified key selector function
    let inline toDictionaryCompareElements
        (keySelector: 'Source -> 'Key)
        (elementSelector: 'Source ->'Elm)
        (comparer:'Key)
        (source:'Source)
        =
        Observable.ToDictionary(
            source,
            Func<'Source,'Key> keySelector,
            Func<'Source,'Elm> elementSelector,
            comparer
        )

    /// Exposes an observable sequence as an object with an Action based .NET event
    let inline toEvent (source:IObservable<unit>) =
        Observable.ToEvent(source)

    /// Exposes an observable sequence as an object with an Action<'Source> based .NET event.
    let inline toEventType ( source:IObservable<'Source> ) : IEventSource<'Source> =
        Observable.ToEvent(source)

    /// Creates a list from an observable sequence
    let inline toList source =
        Observable.ToList(source)

    /// Creates a lookup from an observable sequence according to a specified key selector function.
    let inline toLookup ( keySelector )( source:IObservable<'Source> ) : IObservable<Linq.ILookup<'Key,'Source>> =
        Observable.ToLookup( source, Func<'Source,'Key> keySelector )

    /// Creates a lookup from an observable sequence according to a specified key selector function, and a comparer.
    let inline toLookupCompare ( keySelector ) ( comparer:IEqualityComparer<'Key> )( source:IObservable<'Source> ) : IObservable<Linq.ILookup<'Key,'Source>> =
        Observable.ToLookup( source,Func<'Source,'Key> keySelector, comparer)

    /// Creates a lookup from an observable sequence according to a specified key selector function, and an element selector function.
    let inline toLookupElement ( keySelector ) ( elementSelector ) ( comparer:IEqualityComparer<'Key>)( source:IObservable<'Source> ) : IObservable<Linq.ILookup<'Key,'Element>>=
        Observable.ToLookup( source, Func<'Source,'Key> keySelector, Func<'Source,'Element> elementSelector, comparer )

    /// Creates a lookup from an observable sequence according to a specified key selector function, and an element selector function.
    let inline toLookupCompareElement ( keySelector ) ( elementSelector )( source:IObservable<'Source> ) : IObservable<Linq.ILookup<'Key,'Element>> =
        Observable.ToLookup( source,Func<'Source,'Key>  keySelector, Func<'Source,'Element> elementSelector )

    /// Converts a seq into an observable
    let inline toObservable ( source: seq<'a> ) = Observable.ToObservable(source)

    /// Waits for the observable sequence to complete and returns the last
    /// element of the sequence. If the sequence terminates with OnError
    /// notification, the exception is thrown.
    let inline wait  source =
        Observable.Wait( source )
