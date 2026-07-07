[<AutoOpenAttribute>]
module p1eXu5.FSharp.Reactive.ObservableTransformation

open System
open System.Reactive.Linq

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    /// Converts the elements of the sequence to the specified type
    let inline cast<'CastType> (source) =
        Observable.Cast<'CastType>(source)

    /// Binds an observable to generate a subsequent observable (flatmap duplicate)..
    let inline bind (f: 'a -> IObservable<'TNext>) (m: IObservable<'a>) = m.SelectMany(Func<_,_> f)

    /// Lifts the values of f and m and applies f to m, returning an IObservable of the result.
    let inline apply f m = f |> bind (fun f' -> m |> bind (fun m' -> Observable.Return(f' m')))

    // ------------------------
    // Select (map)
    // ------------------------

    /// Maps the given observable with the given function
    let inline map f source = Observable.Select(source, Func<_,_>(f))


    /// Maps two observables to the specified function.
    let inline map2 f a b = apply (apply f a) b


    /// Combines 'map' and 'fold'. Builds an observable whose emits are the result of applying the given function to each of the emits of the source observable.
    /// The function is also used to accumulate a final value.
    let inline mapFold ([<InlineIfLambda>] f : 'TState -> 'a -> 'TResult * 'TState) (init : 'TState) source =
        Observable.Aggregate(source, ([], init),
            Func<_, _, _> (fun (ys, state) x ->
                let y, state = f state x
                y :: ys, state))

    /// Maps the given observable with the given function and the
    /// index of the element
    ///
    /// Wrapper for:
    ///     Observable.Select(source, f)
    let inline mapi ([<InlineIfLambda>] f: int -> 'Source -> 'Result) (source: IObservable<'Source>) =
        Observable.Select(source, Func<_,_,_>(fun i x -> f x i))

    /// Maps every emission to a constant value.
    let inline mapTo x (source : IObservable<'a>) =
        Observable.Select(source, Func<_, _> (fun _ -> x))

    /// Maps every emission to a constant lazy value.
    let inline mapToLazy (xLazy : Lazy<'a>) (source : IObservable<'a>) =
        Observable.Select(source, Func<_, _> (fun _ -> xLazy.Force ()))

    // ------------------------
    // SelectMany (flatmap)
    // ------------------------

    /// Projects each element of an observable sequence to an observable sequence
    /// and merges the resulting observable sequences into one observable sequence (bind duplicate).
    let inline flatmap binder source =
        Observable.SelectMany(source, Func<'S, IObservable<'R>> binder)

    /// Projects each element of an observable sequence to a async workflow and merges all of the async worksflow results into one observable sequence.
    let inline flatmapAsync asyncOperation (source : IObservable<'Source>) =
        source.SelectMany(fun item -> Observable.liftAsync asyncOperation item)

    /// Projects each element of an observable sequence to an observable sequence by incorporating the
    /// element's index and merges the resulting observable sequences into one observable sequence.
    //    let flatmapi map source =
    //        Observable.SelectMany(source,Func<'Source,int,seq<'Result>> map )
    //

    /// Projects each element of the source observable sequence to the other observable sequence
    /// and merges the resulting observable sequences into one observable sequence.
    let inline bindOther (other: IObservable<'Other> ) ( source:IObservable<'Source> ): IObservable<'Other> =
        Observable.SelectMany(source, other)


    /// Projects each element of an observable sequence to an enumerable sequence and concatenates
    /// the resulting enumerable sequences into one observable sequence.
    let inline bindSeq map source =
        Observable.SelectMany(source, Func<'Source, seq<'Result>> map)


    /// Projects each element of an observable sequence to an enumerable sequence by incorporating the
    /// element's index and concatenates the resulting enumerable sequences into one observable sequence.
    //    let flatmapSeqi map source =
    //        Observable.SelectMany(source, Func<'Source,int,seq<'Result>> map)
    //
    //

    /// Projects each element of an observable sequence to a task and merges all of the task results into one observable sequence.
    let inline bindTask  ( map ) ( source:IObservable<'Source> ) : IObservable<'Result> =
        Observable.SelectMany( source, Func<'Source,Threading.Tasks.Task<'Result>> map )


    /// Materializes the implicit notifications of an observable sequence as
    /// explicit notification values
    let inline materialize source =
        Observable.Materialize(source)

    /// Dematerializes the explicit notification values of an observable sequence as implicit notifications.
    let inline dematerialize source =
        Observable.Dematerialize(source)