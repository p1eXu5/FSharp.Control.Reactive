[<AutoOpenAttribute>]
module FSharp.Control.Reactive.ObservablePublishing

open System
open System.Reactive
open System.Reactive.Linq
open System.Reactive.Concurrency

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    /// Multicasts the source sequence notifications through the specified subject to
    /// the resulting connectable observable. Upon connection of the connectable
    /// observable, the subject is subscribed to the source exactly one, and messages
    /// are forwarded to the observers registered with the connectable observable.
    /// For specializations with fixed subject types, see Publish, PublishLast, and Replay.
    let inline multicast subject source =
        Observable.Multicast(source, subject)

    /// Multicasts the source sequence notifications through an instantiated subject into
    /// all uses of the sequence within a selector function. Each subscription to the
    /// resulting sequence causes a separate multicast invocation, exposing the sequence
    /// resulting from the selector function's invocation. For specializations with fixed
    /// subject types, see Publish, PublishLast, and Replay.
    let inline multicastMap subjectSelector selector source  =
        Observable.Multicast(source, Func<_> subjectSelector, Func<_,_> selector)

    /// Returns a connectable observable sequence (IConnectableObsevable) that shares
    /// a single subscription to the underlying sequence. This operator is a
    /// specialization of Multicast using a regular Subject
    ///
    /// Wrapper for:
    ///     Observable.Publish(source)
    let inline publish source =
        Observable.Publish(source)

    /// Returns a connectable observable sequence (IConnectableObsevable) that shares
    /// a single subscription to the underlying sequence and starts with the value
    /// initial. This operator is a specialization of Multicast using a regular Subject
    ///
    /// Wrapper for:
    ///     Observable.Publish(source, initial)
    let inline publishInitial (initial:'Source) (source:IObservable<'Source>) =
        Observable.Publish(source, initial)

    /// Returns an observable sequence that is the result of invoking
    /// the selector on a connectable observable sequence that shares a
    /// a single subscription to the underlying sequence. This operator is a
    /// specialization of Multicast using a regular Subject
    ///
    /// Wrapper for:
    ///     Observable.Publish(source, map)
    let inline publishMap (map: IObservable<'Source> -> IObservable<'Result>) (source :IObservable<'Source>) =
        Observable.Publish(source, Func<IObservable<'Source>, IObservable<'Result>> map)

    /// Returns an observable sequence that is the result of
    /// the map on a connectable observable sequence that shares a
    /// a single subscription to the underlying sequence. This operator is a
    /// specialization of Multicast using a regular Subject
    let inline publishInitialMap  ( initial : 'Source  )
                            ( map: IObservable<'Source> -> IObservable<'Result> )
                            ( source  : IObservable<'Source> ) =
        Observable.Publish( source, Func<IObservable<'Source>,IObservable<'Result>> map, initial )

    /// Returns an observable that remains connected to the source as long
    /// as there is at least one subscription to the observable sequence
    /// ( publish an Observable to get a ConnectableObservable )
    ///
    /// Wrapper for:
    ///     Observable.RefCount(source)
    let inline refCount source =
        Observable.RefCount(source)

    /// Returns a connectable observable sequence that shares a single subscription to the
    /// underlying sequence replaying all notifications.
    let inline replay ( source:IObservable<'Source>) : Subjects.IConnectableObservable<'Source> =
        Observable.Replay( source )

    /// Returns a connectable observable sequence that shares a single subscription to the
    /// underlying sequence replaying all notifications.
    let inline replayOn ( sch:IScheduler ) source =
        Observable.Replay( source, sch )

    /// Returns a connectable observable sequence that shares a single subscription to the underlying sequence
    /// replaying notifications subject to a maximum element count for the replay buffer.
    let inline replayBuffer ( bufferSize:int )( source:IObservable<'Source>)  : Subjects.IConnectableObservable<'Source> =
            Observable.Replay( source, bufferSize )

    /// Returns a connectable observable sequence that shares a single subscription to the underlying sequence
    /// replaying notifications subject to a maximum element count for the replay buffer and using the specified
    /// scheduler to do the buffering on.
    let inline replayBufferOn ( sch:IScheduler )( bufferSize:int )( source:IObservable<'Source>)  : Subjects.IConnectableObservable<'Source> =
            Observable.Replay( source, bufferSize, sch )

    /// Returns an observable sequence that is the result of invoking the selector on a connectable observable
    /// sequence that shares a single subscription to the underlying sequence replaying all notifications.
    let inline replayMap ( map )( source:IObservable<'Source>)  : IObservable<'Result> =
            Observable.Replay( source, Func<IObservable<'Source>,IObservable<'Result>> map )


    /// Returns a connectable observable sequence that shares a single subscription to the underlying sequence
    /// replaying notifications subject to a maximum time length for the replay buffer.
    let inline replayWindow  ( window:TimeSpan ) ( source:IObservable<'Source>): Subjects.IConnectableObservable<'Source> =
            Observable.Replay( source, window )

    /// Returns a connectable observable sequence that shares a single subscription to the underlying sequence
    /// replaying notifications subject to a maximum time length for the replay buffer.
    let inline replayWindowOn  (scheduler:Concurrency.IScheduler) ( window:TimeSpan ) ( source:IObservable<'Source>): Subjects.IConnectableObservable<'Source> =
            Observable.Replay( source, window, scheduler )

    /// Returns a connectable observable sequence that shares a single subscription to the underlying sequence
    //  replaying notifications subject to a maximum time length and element count for the replay buffer.
    let inline replayBufferWindow  ( bufferSize:int )( window:TimeSpan )( source:IObservable<'Source>) : Subjects.IConnectableObservable<'Source> =
            Observable.Replay( source, bufferSize, window )

    /// Returns a connectable observable sequence that shares a single subscription to the underlying sequence
    //  replaying notifications subject to a maximum time length and element count for the replay buffer.
    let inline replayBufferWindowOn (scheduler:Concurrency.IScheduler) ( bufferSize:int )( window:TimeSpan )( source:IObservable<'Source>) : Subjects.IConnectableObservable<'Source> =
            Observable.Replay( source, bufferSize, window, scheduler )

    /// Returns an observable sequence that is the result of apply a map to a connectable observable sequence that
    /// shares a single subscription to the underlying sequence replaying notifications subject to
    /// a maximum element count for the replay buffer.
    let inline replayMapBuffer ( map ) ( bufferSize:int )( source:IObservable<'Source>) : IObservable<'Result> =
        Observable.Replay( source, Func<IObservable<'Source>,IObservable<'Result>>map, bufferSize )

    /// Returns an observable sequence that is the result of apply a map to a connectable observable sequence that
    /// shares a single subscription to the underlying sequence replaying notifications subject to
    /// a maximum time length.
    let inline replayMapWindow  ( map)( window:TimeSpan )( source:IObservable<'Source>) : IObservable<'Result> =
        Observable.Replay( source,Func<IObservable<'Source>,IObservable<'Result>>  map, window )

    /// Returns an observable sequence that is the result of apply a map to a connectable observable sequence that
    /// shares a single subscription to the underlying sequence replaying notifications subject to
    /// a maximum time length.
    let inline replayMapWindowOn (scheduler:Concurrency.IScheduler) ( map)( window:TimeSpan )( source:IObservable<'Source>) : IObservable<'Result> =
        Observable.Replay( source,Func<IObservable<'Source>,IObservable<'Result>>  map, window, scheduler )

    /// Returns an observable sequence that is the result of apply a map to a connectable observable sequence that
    /// shares a single subscription to the underlying sequence replaying notifications subject to
    /// a maximum time length and element count for the replay buffer.
    let inline replayMapBufferWindow  ( map )( bufferSize:int ) ( window:TimeSpan ) ( source:IObservable<'Source>): IObservable<'Result> =
        Observable.Replay( source, Func<IObservable<'Source>, IObservable<'Result>> map, bufferSize, window )

    /// Returns an observable sequence that is the result of apply a map to a connectable observable sequence that
    /// shares a single subscription to the underlying sequence replaying notifications subject to
    /// a maximum time length and element count for the replay buffer.
    let inline replayMapBufferWindowOn (scheduler:Concurrency.IScheduler) ( map )( bufferSize:int ) ( window:TimeSpan ) ( source:IObservable<'Source>): IObservable<'Result> =
        Observable.Replay( source, Func<IObservable<'Source>, IObservable<'Result>> map, bufferSize, window, scheduler )

