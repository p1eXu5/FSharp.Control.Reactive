[<AutoOpenAttribute>]
module FSharp.Control.Reactive.ObservableCombinators

open System
open System.Collections.Generic
open System.Reactive
open System.Reactive.Linq
open System.Reactive.Concurrency
open System.Threading

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    /// Takes any number of IObservable<T> sources as inputs, and
    /// waits to see which, if any, first produces some sort of output. As soon as this happens, it immediately
    /// unsubscribes from all of the other sources, and forwards all notifications from the source that reacted
    /// first.
    let inline amb second first = Observable.Amb(first, second)

    /// Takes any number of IObservable<T> sources as inputs, and
    /// waits to see which, if any, first produces some sort of output. As soon as this happens, it immediately
    /// unsubscribes from all of the other sources, and forwards all notifications from the source that reacted
    /// first.
    let inline ambSeq (source: IObservable<'a> seq) = Observable.Amb(source)

    /// Takes any number of IObservable<T> sources as inputs, and
    /// waits to see which, if any, first produces some sort of output. As soon as this happens, it immediately
    /// unsubscribes from all of the other sources, and forwards all notifications from the source that reacted
    /// first.
    let inline ambArray (source: IObservable<'a> array) = Observable.Amb(source)

    /// Matches when both observable sequences have an available value (both duplicate).
    let inline ``and`` second first =
        Observable.And(first, second)

    /// Matches when both observable sequences have an available value (``and`` duplacate).
    let inline both second first =
        Observable.And(first, second)

    /// Adds a single item onto the end of any IObservable<'a>.
    let inline append<'a> (value: 'a) (source: IObservable<'a>) : IObservable<'a> =
        Observable.Append(source, value)

    /// Adds a single item onto the end of any IObservable<'a>.
    let inline appendOn<'a> (scheduler:IScheduler) (value: 'a) (source: IObservable<'a>) : IObservable<'a> =
        Observable.Append(source, value, scheduler)

    /// Uses selector to determine which source in sources to return,
    /// choosing an empty sequence if no match is found
    let inline case selector sources =
        Observable.Case( Func<_> selector, sources )

    /// Uses selector to determine which source in sources to return,
    /// choosing defaulSource if no match is found
    let inline caseDefault selector (defaulSource:IObservable<'Result>) (sources:IDictionary<'Value,IObservable<'Result>>) =
        Observable.Case( Func<'Value> selector, sources, defaulSource )


    /// Uses selector to determine which source in sources to return,
    /// choosing an empty sequence on the specified scheduler if no match is found.
    let inline caseOn (scheduler:IScheduler) selector sources =
        Observable.Case (selector, sources, scheduler)


    /// Concatenates the observable sequences obtained by applying the map for each element in the given enumerable
    let inline collect map (source: 'a seq) : IObservable<'b> =
        Observable.For( source, Func<'a, IObservable<'b>> map )



    /// Produces an enumerable sequence that returns elements collected/aggregated from the source sequence between consecutive iterations.
    /// merge - Merges a sequence element with the current collector
    /// newCollector - Factory to create a new collector object.
    let inline collectMerge newCollector merge source =
        Observable.Collect( source, Func<_> newCollector,Func<_,_,_> merge )


    /// Produces an enumerable sequence that returns elements collected/aggregated from the source sequence between consecutive iterations.
    /// merge - Merges a sequence element with the current collector
    /// getNewCollector - Factory to replace the current collector by a new collector
    /// getInitialCollector - Factory to create the initial collector object.
    let inline collectMergeInit getInitialCollector merge getNewCollector source =
        Observable.Collect( source           , Func<_> getInitialCollector  ,
                            Func<_,_,_> merge, Func<_,_> getNewCollector    )


    /// Merges the specified observable sequences into one observable sequence
    /// whenever either of the observable sequences produces an element.
    let inline combineLatest ( source1 : IObservable<'T1> ) ( source2 : IObservable<'T2> ) =
        Observable.CombineLatest(source1, source2, fun t1 t2 -> (t1, t2) )

    /// Merges the specified observable sequences into one observable sequence by
    /// emmiting a list with the latest source elements of whenever any of the
    /// observable sequences produces an element.
    let inline combineLatestSeq (source :seq<IObservable<'a>> ) : IObservable<IList<'a>> =
        Observable.CombineLatest( source )


    /// Merges the specified observable sequences into one observable sequence by  applying the map
    /// whenever any of the observable sequences produces an element.
    let inline combineLatestArray (source :IObservable<'a>[] )  =
        Observable.CombineLatest( source )


    /// Merges the specified observable sequences into one observable sequence by  applying the map
    /// whenever any of the observable sequences produces an element.
    let inline combineLatestSeqMap (map : IList<'a>-> 'Result) (source : IObservable<'a> seq)  =
        Observable.CombineLatest( source, Func<IList<'a>,'Result> map )


    /// Concatenates the second observable sequence to the first observable sequence
    /// upn the successful termination of the first
    let inline concat (second: IObservable<'a>) (first: IObservable<'a>) =
        Observable.Concat(first, second)


    /// Concatenates all observable sequences within the sequence as long as
    /// the previous observable sequence terminated successfully
    let inline concatSeq (sources:seq<IObservable<'a>>) : IObservable<'a> =
        Observable.Concat(sources)


    /// Concatenates all of the specified  observable sequences as long as
    /// the previous observable sequence terminated successfully
    let inline concatArray (sources:IObservable<'a>[]) =
        Observable.Concat(sources)


    /// Concatenates all of the inner observable sequences as long as
    /// the previous observable sequence terminated successfully
    let inline concatInner (sources: IObservable<IObservable<'a>>) =
        Observable.Concat(sources)


    /// Concatenates all task results as long as
    /// the previous task terminated successfully.
    let inline concatTasks(sources: IObservable<Tasks.Task<'a>>) =
        Observable.Concat(sources)

    /// Returns the elements of the specified sequence or the type parameter's default value
    /// in a singleton sequence if the sequence is empty.
    let inline defaultIfEmpty    ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.DefaultIfEmpty( source )


    /// Returns the elements of the specified sequence or the specified value in a singleton sequence if the sequence is empty.
    let inline defaultIfEmptyIs (defaultValue:'Source )( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.DefaultIfEmpty( source, defaultValue )

    /// Determines whether two sequences are equal by comparing the elements pairwise.
    let inline equals ( first:IObservable<'Source>  )( second:IObservable<'Source> ) : IObservable<bool> =
        Observable.SequenceEqual( first, second )


    /// Determines whether two sequences are equal by comparing the elements pairwise using a specified equality comparer.
    let inline equalsComparer ( comparer:IEqualityComparer<'Source>)  ( first:IObservable<'Source>  )( second:IObservable<'Source> ): IObservable<bool> =
        Observable.SequenceEqual( first, second, comparer )


    /// Determines whether an observable and enumerable sequence are equal by comparing the elements pairwise.
    let inline equalsSeq ( first:IObservable<'Source>  )( second:seq<'Source>) : IObservable<bool> =
        Observable.SequenceEqual( first, second )


    /// Determines whether an observable and enumerable sequence are equal by comparing the elements pairwise using a specified equality comparer.
    let inline equalsSeqComparer ( comparer:IEqualityComparer<'Source> ) ( first:IObservable<'Source>  )( second:seq<'Source> ) : IObservable<bool> =
        Observable.SequenceEqual( first, second, comparer )

    /// Correlates the elements of two sequences based on overlapping
    /// durations and groups the results
    let inline groupJoin
        (left: IObservable<'left>)
        (right: IObservable<'right>)
        (leftDurationSelector: 'left -> IObservable<'leftdur> )
        (rightDurationSelector : 'right-> IObservable<'rightdur> )
        (resultSelector: 'left -> IObservable<'right> -> 'result)
        =
        Observable.GroupJoin(
            left,
            right,
            Func<'left , IObservable<'leftdur>> leftDurationSelector,
            Func<'right, IObservable<'rightdur>> rightDurationSelector,
            Func<'left , IObservable<'right>, 'result> resultSelector)

    /// Correlates the elements of two sequences based on overlapping durations.
    let inline join right f left =
        Observable.Join( left, right, Func<_, _> Observable.Return, Func<_, _> Observable.Return, Func<_, _, _> f )


    /// Correlates the elements of two sequences based on overlapping durations.
    let inline joinMap right fLeft fRight (fResult: 'left -> 'right -> 'result) left =
        Observable.Join( left, right, Func<_, _> fLeft, Func<_, _> fRight, Func<_, _, _> fResult )

    /// Merges the two observables
    let inline merge (second: IObservable<'a>) (first: IObservable<'a>) =
        Observable.Merge(first, second)


    /// Merges the two observables, using a specified scheduler for enumeration of and subscription to the sources.
    let inline mergeOn (scheduler:IScheduler) (second:IObservable<'a>) (first:IObservable<'a>) =
        Observable.Merge(first, second, scheduler)


    /// Merges all the observable sequences into a single observable sequence.
    let inline mergeArray (sources:IObservable<'a>[]) =
        Observable.Merge(sources)


    /// Merges all the observable sequences into a single observable sequence,
    /// using a specified scheduler for enumeration of and subscription to the sources.
    let inline mergeArrayOn (scheduler:IScheduler) (sources:IObservable<'a>[]) =
        Observable.Merge(scheduler, sources)


    /// Merges elements from all inner observable sequences
    /// into a single  observable sequence.
    let inline mergeInner (sources:IObservable<IObservable<'a>>) =
        Observable.Merge(sources)


    /// Merges elements from all inner observable sequences
    /// into a single  observable sequence limiting the number of concurrent
    /// subscriptions to inner sequences.
    ///
    /// A maxConcurrent of 1 makes Merge behave in the same way as Concat.
    let inline mergeInnerMax (maxConcurrent:int) (sources:IObservable<IObservable<'a>>) =
        Observable.Merge(sources, maxConcurrent)


    /// Merges an enumerable sequence of observable sequences into a single observable sequence.
    let inline mergeSeq (sources: IObservable<'a> seq) =
        Observable.Merge(sources)


    /// Merges an enumerable sequence of observable sequences into a single observable sequence,
    /// using a specified scheduler for enumeration of and subscription to the sources.
    let inline mergeSeqOn (scheduler:IScheduler) (sources: IObservable<'a> seq) =
        Observable.Merge(sources, scheduler)


    /// Merges an enumerable sequence of observable sequences into an observable sequence,
    /// limiting the number of concurrent subscriptions to inner sequences.
    ///
    /// A maxConcurrent of 1 makes Merge behave in the same way as Concat.
    let inline mergeSeqMax (maxConcurrent:int)(sources:seq<IObservable<'a>>) =
        Observable.Merge(sources, maxConcurrent)


    /// Merges an enumerable sequence of observable sequences into an observable sequence,
    /// limiting the number of concurrent subscriptions to inner sequences,
    /// using a specified scheduler for enumeration of and subscription to the sources.
    ///
    /// A maxConcurrent of 1 makes Merge behave in the same way as Concat.
    let inline mergeSeqMaxOn<'a> (scheduler:IScheduler) (maxConcurrent:int) (sources: IObservable<'a> seq) =
        Observable.Merge(sources, maxConcurrent, scheduler)


    /// Merge results from all source tasks into a single observable sequence
    let inline mergeTasks<'a> (sources:IObservable<Tasks.Task<'a>>) =
        Observable.Merge(sources)

    /// Adds a single item onto the beginning of any IObservable<'a>.
    let inline prepend<'a> (value: 'a) (source: IObservable<'a>) : IObservable<'a> =
        Observable.Prepend(source, value)

    /// Adds a single item onto the beginning of any IObservable<'a>.
    let inline prependOn<'a> (scheduler:IScheduler) (value: 'a) (source: IObservable<'a>) : IObservable<'a> =
        Observable.Prepend(source, value, scheduler)

    /// Repeats the observable sequence indefinitely.
    let inline repeat<'a> (source: IObservable<'a>) : IObservable<'a> =
        Observable.Repeat(source=source)

    /// Repeats the observable sequence a specified number of times.
    let inline repeatCount<'a> (repeatCount: int) (value: 'a) : IObservable<'a> =
        Observable.Repeat(value=value, repeatCount=repeatCount)

    /// Generates an observable sequence that repeats the given element infinitely.
    let inline repeatValue<'a> (value: 'a) : IObservable<'a> =
        Observable.Repeat(value=value)

    /// <summary>
    /// Is a generalization of <see cref="prepend"/> that enables us to provide any number of values to emit
    /// immediately upon subscription. .
    /// </summary>
    let inline startWith<'a> (values: 'a seq) (source: IObservable<'a>) : IObservable<'a> =
        // TODO: re-evaluate wrapping the overload that takes a params array when params are supported by F#.
        Observable.StartWith( source, values )


    /// <summary>
    /// Is a generalization of <see cref="prepend"/> that enables us to provide any number of values to emit
    /// immediately upon subscription. .
    /// </summary>
    let inline startWithOn<'a> (scheduler:IScheduler) (values: 'a seq) (source: IObservable<'a>) : IObservable<'a> =
        Observable.StartWith(source, scheduler, values)

    /// Transforms an observable sequence of observable sequences into an
    /// observable sequence producing values only from the most recent
    /// observable sequence.Each time a new inner observable sequnce is recieved,
    /// unsubscribe from the previous inner sequence
    let inline switch (sources:IObservable<IObservable<'Source>>) : IObservable<'Source>=
        Observable.Switch(sources)


    /// Transforms an observable sequence of tasks into an observable sequence
    /// producing values only from the most recent observable sequence.
    /// Each time a new task is received, the previous task's result is ignored.
    let inline switchTask<'a> (sources: IObservable<Threading.Tasks.Task<'a>>) : IObservable<'a> =
        Observable.Switch(sources)


    /// Transforms an observable sequence of F# Asyncs into an observable sequence
    /// producing values only from the most recent Async.
    /// Each time a new Async is received, the previous Async is cancelled,
    /// and will not continue to run in the background.
    let inline switchAsync<'a> (sources: IObservable<Async<'a>>) : IObservable<'a> =
        Observable.Switch(sources |> Observable.map Observable.fromAsync)
    
    /// **Description**
    /// Projects each source value to an Observable which is merged in the output
    /// Observable, emitting values only from the most recently projected Observable.
    /// Equivalent to a map followed by a switch.
    ///
    /// **Returns**
    /// Returns an Observable that emits items based on applying a function that you
    /// supply to each item emitted by the source Observable, where that function
    /// returns an (so-called "inner") Observable. Each time it observes one of these
    /// inner Observables, the output Observable begins emitting the items emitted by
    /// that inner Observable. When a new inner Observable is emitted, `switchMap`
    /// stops emitting items from the earlier-emitted inner Observable and begins
    /// emitting items from the new one. It continues to behave like this for
    /// subsequent inner Observables.
    let inline switchMap f source =
        source |> Observable.map f |> switch

    /// matches when the observable sequence has an available element and
    /// applies the map (thenMap duplicate).
    let inline ``then`` map source =
        Observable.Then(source, Func<'Source,'Result> map )

    /// matches when the observable sequence has an available element and
    /// applies the map (``then`` duplicate).
    let inline thenMap map source =
        Observable.Then(source, Func<'Source,'Result> map )

    /// Merges two observable sequences into one observable sequence of pairs.
    let inline zip ( first:IObservable<'Source1> ) ( second:IObservable<'Source2> ) : IObservable<'Source1 * 'Source2> =
        Observable.Zip( first, second, fun a b -> a,b)


    /// Merges three observable sequences into one observable sequence of triples.
    let inline zip3 ( first:IObservable<'Source1> ) ( second:IObservable<'Source2> ) ( third:IObservable<'Source3> ) : IObservable<'Source1 * 'Source2 * 'Source3> =
        Observable.Zip( first, second, third, fun a b c -> a,b,c)


    /// Merges two observable sequences into one observable sequence by combining their elements through a projection function.
    let inline zipWith ( resultSelector:'Source1 -> 'Source2 -> 'Result) ( first:IObservable<'Source1>) ( second:IObservable<'Source2>)  : IObservable<'Result> =
        Observable.Zip( first, second, Func<'Source1,'Source2,'Result> resultSelector)


    /// Merges the specified observable sequences into one observable sequence by emitting a
    ///  list with the elements of the observable sequences at corresponding indexes.
    let inline zipSeq ( sources:seq<IObservable<'Source>>) : IObservable<IList<'Source>> =
        Observable.Zip( sources )


    /// Merges the specified observable sequences into one observable sequence by emitting
    /// a list with the elements of the observable sequences at corresponding indexe
    let inline zipArray ( sources:IObservable<'Source> []) : IObservable<IList<'Source>> =
        Observable.Zip( sources )


    /// Merges the specified observable sequences into one observable sequence by using
    /// the selector function whenever all of the observable sequences have produced an
    /// element at a corresponding index.
    let inline zipSeqMap ( resultSelector: IList<'S> ->'R) ( sources: seq<IObservable<'S>>)  : IObservable<'R> =
        Observable.Zip( sources, Func<IList<'S>,'R> resultSelector)



    /// Merges an observable sequence and an enumerable sequence into one
    /// observable sequence by using the selector function.
    let inline zipWithSeq ( resultSelector: 'Source1 -> 'Source2 -> 'Result   )
                    ( second        : seq<'Source2>                       )
                    ( first         : IObservable<'Source1>               ) : IObservable<'Result> =
        Observable.Zip(first, second, Func<_,_,_> resultSelector )


    /// Joins together the results from several patterns (``when`` duplicate).
    let inline joinWhen (plans: Joins.Plan<'a> seq) : IObservable<'a> =
        Observable.When(plans)

    /// Joins together the results from several patterns (joinWhen duplicate).
    let inline ``when`` (plans: Joins.Plan<'a> seq) : IObservable<'a> =
        Observable.When(plans)

    /// Merges the specified observable sequences into one observable sequence by using the selector function
    /// only when the first observable sequence produces an element and there was some element produced by the second
    /// observable sequence
    let inline withLatestFrom resultSelector second first =
        Observable.WithLatestFrom( first, second, Func<_,_,_> resultSelector )