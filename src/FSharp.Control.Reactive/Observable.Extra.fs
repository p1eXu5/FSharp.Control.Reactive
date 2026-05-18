[<AutoOpenAttribute>]
module FSharp.Control.Reactive.ObservableExtra

open System
open System.Reactive.Concurrency
open System.Reactive.Disposables
open System.Reactive.Linq
open FSharp.Control.Reactive

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    /// **Description**
    /// Periodically repeats the observable sequence exposing a responses or failures.
    ///
    /// **Parameters**
    /// - `sch` - Accepts the scheduler on which the polling period should be run.
    /// - `period` - Accepts the period in which the polling should happen.
    /// - `source` - The source observable on which the polling happen.
    let pollOn sch period source =
        Observable.timerOn sch period
        |> Observable.bind (fun _ -> source)
        |> Observable.catchWith (Result.Error >> Observable.retn)
        |> Observable.repeat

    /// **Description**
    /// Periodically repeats the observable sequence exposing a responses or failures.
    /// Using the `Scheduler.Default` on which the polling period should run.
    ///
    /// **Parameters**
    /// - `period` - Accepts the period in which the polling should happen.
    /// - `source` - The source observable on which the polling happen.
    let inline poll period source =
        pollOn Scheduler.Default period source

    /// **Description**
    /// Applies the given function to each value of the given source Observable
    /// comprimised of the results x for each element for which the function returns `Some(x)`.
    ///
    /// **Parameters**
    /// - `f` - Accepts a chooser function to only pick a subset of emits.
    /// - `source` - The source observable to take a subset from.
    let inline choose f source =
        Observable.Create (fun (o : IObserver<_>) ->
            Observable.subscribeSafeWithCallbacks
                (fun x -> Option.iter o.OnNext (try f x with ex -> o.OnError ex; None))
                o.OnError
                o.OnCompleted
                source)

    /// Projects each source value to an Observable which is merged in the output
    /// Observable only if the previous projected Observable has completed.
    //
    /// **Returns**
    /// Returns an Observable that emits items based on applying a function that you
    /// supply to each item emitted by the source Observable, where that function
    /// returns an (so-called "inner") Observable. When it projects a source value to
    /// an Observable, the output Observable begins emitting the items emitted by
    /// that projected Observable. However, `exhaustMap` ignores every new projected
    ///  Observable if the previous projected Observable has not yet completed. Once
    /// that one completes, it will accept and flatten the next projected Observable
    /// and repeat this process.
    let exhaustMap f source =
        Observable.Create (fun (o : IObserver<_>) ->
            let mutable hasSubscription = false
            let mutable innerSub = None
            let onInnerCompleted () =
                hasSubscription <- false
                innerSub |> Option.iter Disposable.dispose
            let onOuterNext x =
                if not hasSubscription then
                    hasSubscription <- true
                    f x |> Observable.subscribeSafeWithCallbacks
                            o.OnNext o.OnError onInnerCompleted
                        |> fun y -> innerSub <- Some y
            source
            |> Observable.subscribeSafeWithCallbacks
                onOuterNext o.OnError o.OnCompleted)

(***************************************************************
    * F# advanced combinator wrappers for Rx extensions
    >> Inspired by the C# Rxx: https://github.com/RxDave/Rxx <<
    ***************************************************************)

    let private serveInternal sch max callOnError sourceFactory =
        Observable.Create (fun (o : IObserver<'a>) ->
            let subscribeSource self =
                let onError ex =
                    if callOnError ex then o.OnError ex
                    else self ()
                sourceFactory ()
                |> Observable.subscribeSafeWithCallbacks
                    o.OnNext onError self

            let com = Disposable.Composite
            for _ in 1..max do
                let current = new SerialDisposable ()
                com.Add current
                sch |> Schedule.actionRec (fun self ->
                        current
                        |> Disposable.setIndirectly
                            (fun () -> subscribeSource self))
                        |> com.Add
            com :> IDisposable)

    /// **Description**
    /// Concurrently invokes the specified factory to create observables as fast and often as possible and subscribes to all of them
    /// up to the default maximum concurrency.
    ///
    /// **Parameters**
    /// - `sch` - The scheduler on which the invocation should happen.
    /// - `gate` - Common gate on which the synchronization should happen.
    /// - `max` - The maximum number of observables to be subscribed simultaneously.
    /// - `callOnError` - Function to determine whether or not the given exception should call the `OnError`.
    /// - `sourceFactory` - Function that returns the observable to invoke concurrently.
    let serveGateCustomOn sch gate max callOnError sourceFactory =
        serveInternal sch max callOnError sourceFactory |> Observable.synchronizeGate gate

    /// **Description**
    /// Concurrently invokes the specified factory to create observables as fast and often as possible and subscribes to all of them
    /// up to the default maximum concurrency.
    ///
    /// **Parameters**
    /// - `sch` - The scheduler on which the invocation should happen.
    /// - `max` - The maximum number of observables to be subscribed simultaneously.
    /// - `callOnError` - Function to determine whether or not the given exception should call the `OnError`.
    /// - `sourceFactory` - Function that returns the observable to invoke concurrently.
    let serveCustomOn sch max callOnError sourceFactory =
        serveInternal sch max callOnError sourceFactory |> Observable.synchronize

    /// **Description**
    /// Concurrently invokes the specified factory to create observables as fast and often as possible and subscribes to all of them
    /// up to the default maximum concurrency.
    ///
    /// **Parameters**
    /// - `sch` - The scheduler on which the invocation should happen.
    /// - `gate` - Common gate on which the synchronization should happen.
    /// - `max` - The maximum number of observables to be subscribed simultaneously.
    /// - `sourceFactory` - Function that returns the observable to invoke concurrently.
    let serveOn sch max sourceFactory =
        serveCustomOn sch max (fun _ -> true) sourceFactory

    /// **Description**
    /// Concurrently invokes the specified factory to create observables as fast and often as possible and subscribes to all of them
    /// up to the default maximum concurrency.
    ///
    /// **Parameters**
    /// - `max` - The maximum number of observables to be subscribed simultaneously.
    /// - `sourceFactory` - Function that returns the observable to invoke concurrently.
    let serve max sourceFactory =
        serveOn Scheduler.CurrentThread max sourceFactory

    /// Generates a sequence using the producer/consumer pattern.
    /// The purpose of the source sequence is simply to notify the consumer when out-of-band data becomes available.
    /// The data in the source sequence provides additional information to the function,
    /// but it does not have to be the actual data being produced.
    ///
    /// The function is not necessarily called for every value in the source sequence.
    /// It is only called if the previous consumer's observable has completed; otherwise, the current notification is ignored.  This ensures
    /// that only one consumer is active at any given time, but it also means that the function is not guaranteed
    /// to receive every value in the source sequence; therefore, the function must read
    /// data from out-of-band storage instead; e.g., from a shared stream or queue.
    ///
    /// The function may also be called when data is not available.  For example, if the current consuming
    /// observable completes and additional notifications from the source were received, then the function
    /// is called again to check whether new data was missed. This avoids a race condition between the source sequence
    /// and the consuming observable's completion notification. If no data is available when function is called, then
    /// an empty sequence should be returned and the function will not be called again until another notification is observed
    /// from the source.
    ///
    /// Producers and the single active consumer are intended to access shared objects concurrently, yet it remains their responsibility
    /// to ensure thread-safety. The consume operator cannot do so without breaking concurrency. For example,
    /// a producer/consumer implementation that uses an in-memory queue must manually ensure that reads and writes to the queue are thread-safe.
    ///
    /// Multiple producers are supported.  Simply create an observable sequence for each producer that notifies when data is generated,
    /// merge them together using the merge operator, and use the merged observable as the source argument in the consume operator.
    /// Multiple consumers are supported by calling consume once and then calling subscribe multiple times on the cold observable that is returned.
    /// Just be sure that the source sequence is hot so that each subscription will consume based on the same producers' notifications.
    ///
    /// ## Parameters
    /// - `f` - A function that generates an observable sequence from out-of-band data.
    /// - `source` - Indicates when data becomes available from one or more producers.
    ///
    /// ## Returns
    /// An observable sequence that is the concatenation of all subscriptions to the consumer observable.
    let consumeMap (f : 'a -> IObservable<'b>) (source : IObservable<'a>) =
        Observable.Create (fun (o : IObserver<_>) ->
            let gate = obj ()
            let consumingSubscription = new SerialDisposable ()
            let schedule = new SerialDisposable ()

            let mutable lastSkipped = Unchecked.defaultof<'a>
            let mutable hasSkipped = false
            let mutable consuming = false
            let mutable stopped = false

            let onNext value =
                lock gate (fun () ->
                    if consuming
                    then
                        lastSkipped <- value
                        hasSkipped <- true
                    else
                        consuming <- true
                        hasSkipped <- false)

                let mutable additionalData = value
                let scheduleRec self =
                    let onCompleted () =
                        let consumeAgain, completeNow =
                            lock gate (fun () ->
                                consuming <- hasSkipped
                                if consuming then
                                    additionalData <- lastSkipped
                                    hasSkipped <- false
                                    true, false
                                else false, stopped)

                        if consumeAgain then self ()
                        if completeNow then o.OnCompleted ()

                    try Some (f additionalData)
                    with | ex -> o.OnError ex; None
                    |> Option.iter (fun xs ->
                        consumingSubscription
                        |> Disposable.setIndirectly (fun () ->
                            Observable.subscribeSafeWithCallbacks
                                o.OnNext o.OnError onCompleted xs))

                Scheduler.Immediate
                |> Schedule.actionRec scheduleRec
                |> Disposable.setInnerDisposalOf schedule

            let onCompleted () =
                let completeNow = lock gate (fun () ->
                    stopped <- true; consuming)
                if completeNow then o.OnCompleted ()

            let subscription =
                Observable.subscribeSafeWithCallbacks
                    onNext o.OnError onCompleted source

            Disposables.compose [ consumingSubscription; subscription; schedule ])

    /// Generates a sequence using the producer/consumer pattern.
    /// The purpose of the source sequence is simply to notify the consumer when out-of-band data becomes available.
    /// The data in the source sequence provides additional information to the function,
    /// but it does not have to be the actual data being produced.
    ///
    /// The function is not necessarily called for every value in the source sequence.
    /// It is only called if the previous consumer's observable has completed; otherwise, the current notification is ignored.  This ensures
    /// that only one consumer is active at any given time, but it also means that the function is not guaranteed
    /// to receive every value in the source sequence; therefore, the function must read
    /// data from out-of-band storage instead; e.g., from a shared stream or queue.
    ///
    /// The function may also be called when data is not available.  For example, if the current consuming
    /// observable completes and additional notifications from the source were received, then the function
    /// is called again to check whether new data was missed. This avoids a race condition between the source sequence
    /// and the consuming observable's completion notification. If no data is available when function is called, then
    /// an empty sequence should be returned and the function will not be called again until another notification is observed
    /// from the source.
    ///
    /// Producers and the single active consumer are intended to access shared objects concurrently, yet it remains their responsibility
    /// to ensure thread-safety. The consume operator cannot do so without breaking concurrency. For example,
    /// a producer/consumer implementation that uses an in-memory queue must manually ensure that reads and writes to the queue are thread-safe.
    ///
    /// Multiple producers are supported.  Simply create an observable sequence for each producer that notifies when data is generated,
    /// merge them together using the merge operator, and use the merged observable as the source argument in the consume operator.
    /// Multiple consumers are supported by calling consume once and then calling subscribe multiple times on the cold observable that is returned.
    /// Just be sure that the source sequence is hot so that each subscription will consume based on the same producers' notifications.
    ///
    /// ## Parameters
    /// - `f` - A function that generates an observable sequence from out-of-band data.
    /// - `source` - Indicates when data becomes available from one or more producers.
    ///
    /// ## Returns
    /// An observable sequence that is the concatenation of all subscriptions to the consumer observable.
    let inline consume consumer source = consumeMap (fun _ -> consumer) source

    /// Generates a sequence using the producer/consumer pattern.
    /// The purpose of the source sequence is simply to notify the consumer when out-of-band data becomes available.
    /// The data in the source sequence provides additional information to the function,
    /// but it does not have to be the actual data being produced.
    ///
    /// The function is not necessarily called for every value in the source sequence.
    /// It is only called if the previous consumer's observable has completed; otherwise, the current notification is ignored.  This ensures
    /// that only one consumer is active at any given time, but it also means that the function is not guaranteed
    /// to receive every value in the source sequence; therefore, the function must read
    /// data from out-of-band storage instead; e.g., from a shared stream or queue.
    ///
    /// The function may also be called when data is not available.  For example, if the current consuming
    /// observable completes and additional notifications from the source were received, then the function
    /// is called again to check whether new data was missed. This avoids a race condition between the source sequence
    /// and the consuming observable's completion notification. If no data is available when function is called, then
    /// an empty sequence should be returned and the function will not be called again until another notification is observed
    /// from the source.
    ///
    /// Producers and the single active consumer are intended to access shared objects concurrently, yet it remains their responsibility
    /// to ensure thread-safety. The consume operator cannot do so without breaking concurrency. For example,
    /// a producer/consumer implementation that uses an in-memory queue must manually ensure that reads and writes to the queue are thread-safe.
    ///
    /// Multiple producers are supported.  Simply create an observable sequence for each producer that notifies when data is generated,
    /// merge them together using the merge operator, and use the merged observable as the source argument in the consume operator.
    /// Multiple consumers are supported by calling consume once and then calling subscribe multiple times on the cold observable that is returned.
    /// Just be sure that the source sequence is hot so that each subscription will consume based on the same producers' notifications.
    ///
    /// ## Parameters
    /// - `f` - A function that is called iteratively to generate values from out-of-band data.
    /// - `source` - Indicates when data becomes available from one or more producers.
    ///
    /// ## Returns
    /// An observable sequence that is the concatenation of the values returned by the consumeNext function.
    let consumeNextOn sch f source =
        let rec onNext (o : IObserver<_>) x =
            try f x
            with ex -> o.OnError ex; None
            |> function
                | Some next -> o.OnNext next; onNext o x
                | None -> o.OnCompleted ()
        source
        |> consumeMap (fun x ->
            Observable.Create (fun o ->
                Schedule.action (fun () -> onNext o x) sch))

    /// Generates a sequence using the producer/consumer pattern.
    /// The purpose of the source sequence is simply to notify the consumer when out-of-band data becomes available.
    /// The data in the source sequence provides additional information to the function,
    /// but it does not have to be the actual data being produced.
    ///
    /// The function is not necessarily called for every value in the source sequence.
    /// It is only called if the previous consumer's observable has completed; otherwise, the current notification is ignored.  This ensures
    /// that only one consumer is active at any given time, but it also means that the function is not guaranteed
    /// to receive every value in the source sequence; therefore, the function must read
    /// data from out-of-band storage instead; e.g., from a shared stream or queue.
    ///
    /// The function may also be called when data is not available.  For example, if the current consuming
    /// observable completes and additional notifications from the source were received, then the function
    /// is called again to check whether new data was missed. This avoids a race condition between the source sequence
    /// and the consuming observable's completion notification. If no data is available when function is called, then
    /// an empty sequence should be returned and the function will not be called again until another notification is observed
    /// from the source.
    ///
    /// Producers and the single active consumer are intended to access shared objects concurrently, yet it remains their responsibility
    /// to ensure thread-safety. The consume operator cannot do so without breaking concurrency. For example,
    /// a producer/consumer implementation that uses an in-memory queue must manually ensure that reads and writes to the queue are thread-safe.
    ///
    /// Multiple producers are supported.  Simply create an observable sequence for each producer that notifies when data is generated,
    /// merge them together using the merge operator, and use the merged observable as the source argument in the consume operator.
    /// Multiple consumers are supported by calling consume once and then calling subscribe multiple times on the cold observable that is returned.
    /// Just be sure that the source sequence is hot so that each subscription will consume based on the same producers' notifications.
    ///
    /// ## Parameters
    /// - `f` - A function that is called iteratively to generate values from out-of-band data.
    /// - `source` - Indicates when data becomes available from one or more producers.
    ///
    /// ## Returns
    /// An observable sequence that is the concatenation of the values returned by the consumeNext function.
    let inline consumeNext f source = consumeNextOn Scheduler.Default f source

