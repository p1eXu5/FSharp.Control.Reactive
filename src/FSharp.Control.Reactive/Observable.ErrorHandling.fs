[<AutoOpenAttribute>]
module FSharp.Control.Reactive.ObservableErrorHandling

open System
open System.Reactive.Linq

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    /// Continues an observable sequence that is terminated
    /// by an exception with the next observable sequence.
    let inline catch (second: IObservable<'a>) first =
        Observable.Catch(first, second)


    /// Continues an observable sequence that is terminated by an exception
    /// with an optional as result type.
    let inline catchOption (source : IObservable<_>) =
        let some = source.Select(Func<_, _> Some)
        let none _ = Observable.Return None
        Observable.Catch(some, none)


    /// Continues an observable sequence that is terminated by an exception of
    /// the specified type with the observable sequence produced by the handler,
    /// wrapped in a 'Result' type.
    let inline catchResult handler (source : IObservable<_>)  =
        let normal = source.Select(Func<_, _> Result.Ok)
        let error ex = handler ex |> fun (o : IObservable<_>) -> o.Select(Func<_, _> Result.Error)
        Observable.Catch(normal, error)


    /// Continues an observable sequence that is terminated by an exception of
    /// the specified type with the observable sequence produced by the handler.
    let inline catchWith handler source =
        Observable.Catch( source,Func<_,_> handler )


    /// Continues an observable sequence that is terminated by an exception with the next observable sequence.
    let inline catchSeq (sources:seq<IObservable<'a>>) =
        Observable.Catch(sources)


    /// Continues an observable sequence that is terminated by an exception with the next observable sequence.
    let inline catchArray (sources:IObservable<'a>[]) =
        Observable.Catch(sources)

    /// Invokes a specified action after the source observable sequence
    /// terminates gracefully or exceptionally
    let inline finallyDo finallyAction source =
        Observable.Finally( source, Action finallyAction )

    /// Concatenates the second observable sequence to the first observable sequence
    /// upon successful or exceptional termination of the first.
    let inline onErrorConcat ( second:IObservable<'Source> ) ( first:IObservable<'Source> ) : IObservable<'Source> =
        Observable.OnErrorResumeNext( first, second )

    /// Concatenates all of the specified observable sequences, even if the previous observable sequence terminated exceptionally.
    let inline onErrorConcatArray ( sources:IObservable<'Source> [] ) : IObservable<'Source> =
        Observable.OnErrorResumeNext( sources )

    /// Concatenates all observable sequences in the given enumerable sequence, even if the
    /// previous observable sequence terminated exceptionally.
    let inline onErrorConcatSeq ( sources:seq<IObservable<'Source>> ) : IObservable<'Source> =
        Observable.OnErrorResumeNext( sources )

    /// Repeats the source observable sequence until it successfully terminates.
    let inline retry ( source:IObservable<'Source>) : IObservable<'Source> =
        Observable.Retry( source )


    /// Repeats the source observable sequence the specified number of times or until it successfully terminates.
    let inline retryCount (count:int) ( source:IObservable<'Source>) : IObservable<'Source> =
        Observable.Retry( source, count )

    /// Constructs an observable sequence that depends on a resource object, whose
    /// lifetime is tied to the resulting observable sequence's lifetime.
    let inline using ( resourceFactory: unit ->'TResource ) (observableFactory: 'TResource -> IObservable<'Result> ) : IObservable<'Result> =
        Observable.Using ( Func<_> resourceFactory, Func<_,_> observableFactory )


    /// Constructs an observable sequence that depends on a resource object, whose
    /// lifetime is tied to the resulting observable sequence's lifetime.
    /// The resource is obtained and used through asynchronous functions.
    /// The cancellation token passed to the asyncrhonous functions is tied to the returned disposable subscription,
    /// allowing best-effor cancellation at any stage of the resource acquisition or usage.
    let inline usingAsync resourceFactory observableFactory =
        Observable.Using ( Func<_, _> (resourceFactory >> Async.StartAsTask), Func< _, _, _> (fun d ct -> observableFactory d ct |> Async.StartAsTask))
