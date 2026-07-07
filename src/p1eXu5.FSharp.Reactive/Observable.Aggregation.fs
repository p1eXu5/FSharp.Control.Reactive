[<AutoOpenAttribute>]
module p1eXu5.FSharp.Reactive.ObservableAggregation

open System
open System.Reactive.Linq

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    /// Counts the elements
    let inline count source =
        Observable.Count(source)

    /// Returns an observable sequence containing an int that represents how many elements
    /// in the specified observable sequence satisfy a condition.
    let inline countSatisfy predicate source =
        Observable.Count(source, Func<_,_> predicate)

    /// Returns an observable sequence containing a int64 that represents
    /// the total number of elements in an observable sequence
    let inline countInt64 source =
        Observable.LongCount(source)


    /// Returns an observable sequence containing an int that represents how many elements
    /// in the specified observable sequence satisfy a condition.
    let inline countInt64Satisfy predicate source =
        Observable.LongCount(source, Func<_,_> predicate)

    let inline sum (source: IObservable<int>) =
        Observable.Sum(source)

    let inline sumInt64 (source: IObservable<int64>) =
        Observable.Sum(source)

    let inline sumDecimal (source: IObservable<decimal>) =
        Observable.Sum(source)

    let inline average (source: IObservable<int>) =
        Observable.Average(source)

    let inline averageInt64 (source: IObservable<int64>) =
        Observable.Average(source)

    let inline averageDecimal (source: IObservable<decimal>) =
        Observable.Average(source)

    let inline min (source: IObservable<int>) =
        Observable.Min(source)

    let inline minInt64 (source: IObservable<int64>) =
        Observable.Min(source)

    let inline minDecimal (source: IObservable<decimal>) =
        Observable.Min(source)

    let inline max (source: IObservable<int>) =
        Observable.Max(source)

    let inline maxInt64 (source: IObservable<int64>) =
        Observable.Max(source)

    let inline maxDecimal (source: IObservable<decimal>) =
        Observable.Max(source)

    let inline minBy<'a,'key> keySelector (source: IObservable<'a>) =
        Observable.MinBy(source, Func<'a,'key> keySelector)

    let inline minByComp<'a,'key> keySelector comparator (source: IObservable<'a>) =
        Observable.MinBy(source, Func<'a,'key> keySelector, comparator)

    let inline maxBy<'a,'key> keySelector (source: IObservable<'a>) =
        Observable.MaxBy(source, Func<'a,'key> keySelector)

    let inline maxByComp<'a,'key> keySelector comparator (source: IObservable<'a>) =
        Observable.MaxBy(source, Func<'a,'key> keySelector, comparator)

    /// Returns the maximum element in an observable sequence.
    let inline maxOf (source:IObservable<'a>) =
        Observable.Max( source )

    /// Determines whether an observable sequence contains a specified value
    /// which satisfies the given predicate (`any` duplicate).
    let inline exists (source:IObservable<'Source>) : IObservable<bool> =
        Observable.Any(source)

    /// Determines whether an observable sequence contains a specified value
    /// which satisfies the given predicate (`exists` duplicate).
    let inline any (source:IObservable<'Source>) : IObservable<bool> =
        Observable.Any(source)

    /// Determines whether all elements of an observable satisfy a predicate
    let inline all pred source =
        Observable.All(source, Func<_,_> pred)

    /// IsEmpty returns an Observable that emits true if and only if the
    /// source Observable completes without emitting any items.
    let inline isEmpty<'a> (source: IObservable<'a>) =
        Observable.IsEmpty(source)

    /// Determines whether an observable sequence contains a specified
    /// element by using the default equality comparer.
    let inline contains<'a> (value: 'a) (source: IObservable<'a>) =
        Observable.Contains(source, value)

    /// Determines whether an observable sequence contains a
    /// specified element by using a specified EqualityComparer
    let inline containsComp<'a> (value: 'a) comparator (source: IObservable<'a>) =
        Observable.Contains(source, value, comparator)


    /// Applies an accumulator function over an observable sequence, returning the
    /// result of the aggregation as a single element in the result sequence
    let inline aggregate accumulator source =
        Observable.Aggregate(source, Func<_,_,_> accumulator )

    /// Applies an accumulator function over an observable sequence, returning the
    /// result of the fold as a single element in the result sequence
    /// init is the initial accumulator value
    let inline fold accumulator init source =
        Observable.Aggregate(source, init, Func<_,_,_> accumulator)

    /// Applies an accumulator function over an observable sequence, returning the
    /// result of the fold as a single element in the result sequence
    /// init is the initial accumulator value, map is performed after the fold
    let inline foldMap accumulator init map source =
        Observable.Aggregate(source, init,Func<_,_,_> accumulator,Func<_,_>  map )

    /// Applies an accumulator function over an observable sequence and returns each intermediate result.
    let inline scan (accumulator:'a->'a->'a)  source =
        Observable.Scan(source, Func<'a,'a,'a> accumulator  )


    /// Applies an accumulator function over an observable sequence and returns each intermediate result.
    /// The specified init value is used as the initial accumulator value.
    let inline scanInit (init:'TAccumulate) (accumulator) (source:IObservable<'Source>) : IObservable<'TAccumulate> =
        Observable.Scan( source, init, Func<'TAccumulate,'Source,'TAccumulate> accumulator )

    /// Reduces the observable
    let inline reduce f source = Observable.Aggregate(source, Func<_,_,_> f)