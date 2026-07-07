[<AutoOpenAttribute>]
module p1eXu5.FSharp.Reactive.ObservableFilters

open System
open System.Reactive.Linq
open System.Reactive.Concurrency
open System.Collections.Generic

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    // ------------------------
    // Distinct
    // ------------------------

    /// Returns an observable sequence that only contains distinct elements
    let inline distinct ( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.Distinct( source )


    /// Returns an observable sequence that contains only distinct elements according to the keySelector.
    let inline distinctKey ( keySelector:'Source -> 'Key )( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.Distinct( source, Func<'Source,'Key> keySelector)


    /// Returns an observable sequence that contains only distinct elements according to the comparer.
    let inline distinctCompare<'Source> ( comparer: IEqualityComparer<'Source> )( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.Distinct( source, comparer )


    /// Returns an observable sequence that contains only distinct elements according to the keySelector and the comparer.
    let inline distinctKeyCompare ( keySelector:'Source -> 'Key )( comparer:IEqualityComparer<'Key>)( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.Distinct( source, Func<'Source,'Key> keySelector, comparer )


    /// Returns an observable sequence that only contains distinct contiguous elements
    let inline distinctUntilChanged ( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.DistinctUntilChanged(source)


    /// Returns an observable sequence that contains only distinct contiguous elements according to the keySelector.
    let inline distinctUntilChangedKey ( keySelector:'Source -> 'Key )( source:IObservable<'Source> )  : IObservable<'Source> =
        Observable.DistinctUntilChanged( source, Func<'Source,'Key> keySelector )


    /// Returns an observable sequence that contains only distinct contiguous elements according to the comparer.
    let inline distinctUntilChangedCompare ( comparer:IEqualityComparer<'Source> )( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.DistinctUntilChanged( source, comparer )


    /// Returns an observable sequence that contains only distinct contiguous elements according to the keySelector and the comparer.
    let inline distinctUntilChangedKeyCompare  ( keySelector:'Source -> 'Key )( comparer:IEqualityComparer<'Key> )( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.DistinctUntilChanged( source, Func<'Source,'Key> keySelector, comparer )

    // ------------------------
    // ElementAt
    // ------------------------

    /// Returns the element at a specified index in a sequence.
    let inline elementAt (index:int) (source:IObservable<'Source>) : IObservable<'Source> =
        Observable.ElementAt( source, index )

    /// Returns the element at a specified index in a sequence or a default value if the index is out of range
    let inline elementAtOrDefault (index:int) (source:IObservable<'Source>) : IObservable<'Source> =
        Observable.ElementAtOrDefault(source, index)

    // ------------------------
    // Where (filter)
    // ------------------------

    /// Filters the observable elements of a sequence based on a predicate
    let inline filter predicate (source: IObservable<'a>) =
        Observable.Where( source, Func<_,_> predicate )


    /// Filters the observable elements of a sequence based on a predicate by
    /// incorporating the element's index
    let inline filteri predicate (source: IObservable<'a>)  =
        Observable.Where(
            source,
            Func<_,_,_> (fun i x -> predicate x i)
        )

    /// Ignores all elements in an observable sequence leaving only the completed/error notifications
    let inline ignoreElements source =
        Observable.IgnoreElements(source)

    /// Filters the elements of an observable sequence based on the specified type
    let inline ofType source =
        Observable.OfType(source)

    /// Returns the first element of an observable sequence
    let inline first (source:IObservable<'a>)  =
        source.FirstAsync()

    /// Takes the first element of the observable sequence
    let inline head obs = Observable.FirstAsync(obs)

    /// Returns the first element of an observable sequence
    /// if it satisfies the predicate
    let inline firstIf predicate (source:IObservable<'a>) =
        source.FirstAsync(Func<_,_> predicate)

    /// Returns the last element of an observable sequence.
    let inline last ( source:IObservable<'Source>) : IObservable<'Source> =
        Observable.LastAsync( source )


    /// Returns the last element of an observable sequence that satisfies the condition in the predicate
    let inline lastIf  ( predicate ) ( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.LastAsync( source, Func<'Source,bool> predicate )

    

    /// Returns an observable sequence that is the result of invoking
    /// the selector on a connectable observable sequence containing
    /// only the last notification This operator is a
    /// specialization of Multicast using a regular Subject
    let inline publishLast source =
        Observable.PublishLast( source )

    /// Returns an observable sequence that is the result of invoking
    /// the selector on a connectable observable sequence that shares a
    /// a single subscription to the underlying sequence. This operator is a
    /// specialization of Multicast using a regular Subject
    let inline publishLastMap ( map: IObservable<'Source> -> IObservable<'Result> ) source  =
        Observable.PublishLast( source , Func<IObservable<'Source>,IObservable<'Result>> map )

    /// Bypasses a specified number of elements in an observable sequence and then returns the remaining elements.
    let inline skip (count:int) (source:IObservable<'Source>)  : IObservable<'Source> =
        Observable.Skip(source , count)


    /// Skips elements for the specified duration from the start of the observable source sequence.
    let inline skipSpan  (duration:TimeSpan ) (source:IObservable<'Source> ): IObservable<'Source> =
        Observable.Skip(source, duration)


    /// Skips elements for the specified duration from the start of the observable source sequence,
    /// using a specified scheduler to run timers.
    let inline skipSpanOn (scheduler:IScheduler) duration source =
        Observable.Skip(source, duration, scheduler)


    /// Bypasses a specified number of elements at the end of an observable sequence.
    let inline skipLast  (count:int ) ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.SkipLast (source, count )


    /// Skips elements for the specified duration from the end of the observable source sequence.
    let inline skipLastSpan (duration:TimeSpan ) ( source:IObservable<'Source>) : IObservable<'Source> =
        Observable.SkipLast ( source, duration)


    /// Skips elements for the specified duration from the end of the observable source sequence,
    /// using the specified scheduler to run timers.
    let inline skipLastSpanOn (scheduler:IScheduler) duration source =
        Observable.SkipLast(source, duration, scheduler)


    /// Skips elements from the observable source sequence until the specified start time.
    let inline skipUntil (startTime:DateTimeOffset ) ( source:IObservable<'Source> )  : IObservable<'Source> =
        Observable.SkipUntil(source, startTime )


    /// Skips elements from the observable source sequence until the specified start time,
    /// using the specified scheduler to run timers.
    let inline skipUntilOn (scheduler:IScheduler) startTime source =
        Observable.SkipUntil(source, startTime, scheduler)


    /// Returns the elements from the source observable sequence only after the other observable sequence produces an element.
    let inline skipUntilOther ( other:IObservable<'Other> )  ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.SkipUntil(source, other )



    /// Bypasses elements in an observable sequence as long as a specified condition is true and then returns the remaining elements.
    let inline skipWhile ( predicate:'Source -> bool ) ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.SkipWhile ( source, Func<'Source,bool> predicate )


    /// Bypasses elements in an observable sequence as long as a specified condition is true and then returns the remaining elements.
    /// The element's index is used in the logic of the predicate functio
    let inline skipWhilei ( predicate:'Source -> int -> bool)( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.SkipWhile ( source, Func<'Source,int,bool> predicate)


    /// Takes n elements (from the beginning of an observable sequence? )
    let inline take (n: int) source : IObservable<'Source> =
        Observable.Take(source, n)

    /// Returns a specified number of contiguous elemenents from the start of an observable sequence,
    /// using the specified scheduler for the edge case of take(0).
    let inline takeOn (scheduler:IScheduler) (n:int) source =
        Observable.Take( source, n, scheduler )


    /// Takes elements for a specified duration from the start of the observable source sequence.
    let inline takeSpan (duration:TimeSpan) source =
        Observable.Take( source, duration )


    /// Takes elements for a specified duration from the start of the observable source sequence,
    /// using the specified scheduler to run timers.
    let inline takeSpanOn (scheduler:IScheduler) (duration:TimeSpan) source =
        Observable.Take( source, duration, scheduler )


    /// Returns a specified number of contiguous elements from the end of an obserable sequence
    let inline takeLast ( count:int ) source =
        Observable.TakeLast(source, count)


    /// Returns a specified number of contiguous elements from the end of an obserable sequence,
    /// using the specified scheduler to drain the queue.
    let inline takeLastOn (scheduler:IScheduler) (count:int) source =
        Observable.TakeLast( source, count, scheduler )


    /// Returns elements within the specified duration from the end of the observable source sequence.
    let inline takeLastSpan ( duration:TimeSpan ) ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.TakeLast( source, duration )


    /// Returns elements within the specified duration from the end of the observable source sequence,
    /// using the specified scheduler to run timers.
    let inline takeLastSpanOn (scheduler:IScheduler) (duration:TimeSpan) source =
        Observable.TakeLast( source, duration, scheduler)


    /// Returns a list with the elements within the specified duration from the end of the observable source sequence.
    let inline takeLastBuffer ( duration:TimeSpan )( source:IObservable<'Source> ): IObservable<Collections.Generic.IList<'Source>> =
        Observable.TakeLastBuffer( source, duration )


    /// Returns a list with the elements within the specified duration from the end of the observable source sequence,
    /// using the specified scheduler to run timers.
    let inline takeLastBufferOn (scheduler:IScheduler) duration source =
        Observable.TakeLastBuffer( source, duration, scheduler )


    /// Returns a list with the specified number of contiguous elements from the end of an observable sequence.
    let inline takeLastBufferCount ( count:int )( source:IObservable<'Source> ): IObservable<Collections.Generic.IList<'Source>> =
        Observable.TakeLastBuffer( source, count )


    /// Returns the elements from the source observable sequence until the other produces and element
    let inline takeUntilOther<'Other,'Source> other source =
        Observable.TakeUntil<'Source,'Other>(source , other )

//
    /// Returns the elements from the source observable until the specified time
    let inline takeUntilTime<'Source> (endtime:DateTimeOffset) source =
        Observable.TakeUntil<'Source>(source , endtime )


    /// Returns the elements from the source observable until the specified time,
    /// using the specified scheduler to run timers.
    let inline takeUntilTimeOn (scheduler:IScheduler) endtime source =
        Observable.TakeUntil( source, endtime, scheduler )


    /// Returns elements from an observable sequence as long as a specified condition is true.
    let inline takeWhile  (predicate) ( source:IObservable<'Source>): IObservable<'Source> =
        Observable.TakeWhile( source, Func<'Source,bool>predicate )


    /// Returns elements from an observable sequence as long as a specified condition is true.
    /// The element's index is used in the logic of the predicate functi
    let inline takeWhilei  ( predicate) (source:IObservable<'Source>) : IObservable<'Source> =
        Observable.TakeWhile( source, Func<'Source,int,bool> predicate )

    /// Bypasses the first element in an observable sequence and then returns the remaining elements.
    let inline tail source = skip 1 source

    /// Repeats the given function as long as the specified condition holds
    /// where the condition is evaluated before each repeated source is
    /// subscribed to
    let inline whileLoop condition source =
        Observable.While( Func<bool> condition, source )
