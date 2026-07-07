[<AutoOpenAttribute>]
module p1eXu5.FSharp.Reactive.ObservableGenerators

open System
open System.Reactive.Linq
open System.Reactive.Concurrency

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    /// Creates a range as an observable
    let inline range start count = Observable.Range(start, count)


    /// Creates a range as an observable, using the specified scheduler to send out observer messages.
    let inline rangeOn (scheduler: IScheduler) start count = Observable.Range(start, count, scheduler)

    
    /// Generates an observable sequence by running a state-driven loop producing the sequence's elements.
    let inline generate initialState condition iterate resultSelector =
        Observable.Generate(                            
            initialState,
            Func<'State,bool> condition,
            Func<'State,'State> iterate,
            Func<'State,'Result> resultSelector
        )

    /// Generates an observable sequence by running a state-driven loop producing the sequence's elements.
    let inline generateOn (scheduler:IScheduler) initialState condition iterate resultSelector =
        Observable.Generate(
            initialState,
            Func<'State, bool> condition,
            Func<'State, 'State> iterate,
            Func<'State, 'TResult> resultSelector,
            scheduler
        )


    /// Generates an observable sequence by running a state-driven and temporal loop producing the sequence's elements.
    let inline generateDateTime (initialState:'State) condition iterate resultSelector timeSelector : IObservable<'Result> =
        Observable.Generate(
            initialState,
            Func<'State,bool> condition,
            Func<'State,'State> iterate,
            Func<'State,'Result> resultSelector,
            Func<'State, DateTimeOffset> timeSelector
        )


    /// Generates an observable sequence by running a state-driven and temporal loop producing the sequence's elements,
    /// using a specified scheduler to run timers and to send out observer messages.
    let inline generateDateTimeOn (scheduler:IScheduler) (initialState:'State) condition iterate resultSelector timeSelector : IObservable<'Result> =
        Observable.Generate(
            initialState,
            Func<'State,bool> condition,
            Func<'State,'State> iterate,
            Func<'State,'Result> resultSelector,
            Func<'State, DateTimeOffset> timeSelector,
            scheduler
        )

    /// Generates an observable sequence by running a state-driven and temporal loop producing the sequence's elements.
    let inline generateTimeSpan (initialState:'State) condition iterate resultSelector timeSelector : IObservable<'Result> =
        Observable.Generate(
            initialState,
            Func<_,_> condition,
            Func<_,_> iterate,
            Func<'State,'Result> resultSelector,
            Func<'State,TimeSpan> timeSelector
        )


    /// Generates an observable sequence by running a state-driven and temporal loop producing the sequence's elements,
    /// using a specified scheduler to run timers and to send out observer messages.
    let inline generateTimeSpanOn (scheduler:IScheduler) (initialState: 'State) condition iterate resultSelector timeSelector : IObservable<'Result> =
        Observable.Generate(
            initialState,
            Func<_,_> condition,
            Func<_,_> iterate,
            Func<'State,'Result> resultSelector,
            Func<'State,TimeSpan> timeSelector,
            scheduler
        )

    /// Samples the observable at the given interval
    let inline sample (interval: TimeSpan) source =
        Observable.Sample(source, interval)

    /// Samples the observable sequence at each interval, using the specified scheduler to run sampling timers.
    /// Upon each sampling tick, the latest element (if any) in the source sequence during the
    /// last sampling interval is sent to the resulting sequence.
    let inline sampleOn scheduler interval source =
        Observable.Sample(source, interval, scheduler)

    /// Samples the source observable sequence using a samper observable sequence producing sampling ticks.
    /// Upon each sampling tick, the latest element (if any) in the source sequence during the
    /// last sampling interval is sent to the resulting sequence.
    let inline sampleWith (sampler:IObservable<'Sample>) (source:IObservable<'Source>) : IObservable<'Source> =
        Observable.Sample(source, sampler)


    /// Returns a single element from the observable sequence.
    /// Throws an exception if the sequence doesn't contain exactly one element.
    let inline single<'a> (source: IObservable<'a>) =
        Observable.SingleAsync(source)

    /// Returns a single element from the observable sequence that matches the specified predicate.
    /// Throws an exception if no elements match or more than one matches.
    let inline singleIf<'a> (predicate: 'a -> bool) source =
        Observable.SingleAsync(source, Func<'a, bool> predicate)

    /// Returns a single element from the observable sequence, or a default value
    /// if the sequence doesn't contain exactly one element.
    let inline singleOrDefault<'a> (source: IObservable<'a>) =
        Observable.SingleOrDefaultAsync(source)

    /// Returns a single element from the observable sequence that matches the specified predicate,
    /// or a default value if no elements match or more than one matches.
    let inline singleIfOrDefault<'a> (predicate: 'a -> bool) source =
        Observable.SingleOrDefaultAsync(source, Func<'a, bool> predicate)
