[<AutoOpenAttribute>]
module FSharp.Control.Reactive.ObservableTimeBased

open System
open System.Reactive
open System.Reactive.Concurrency
open System.Reactive.Linq

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    /// Time shifts the observable sequence by the specified relative time duration.
    /// The relative time intervals between the values are preserved.
    let inline delay ( dueTime:TimeSpan ) ( source:IObservable<'Source> ): IObservable<'Source>=
        Observable.Delay(source, dueTime)

    /// Time shifts the observable sequence by the specified relative time duration.
    /// The relative time intervals between the values are preserved.
    let inline delaySec (dueTimeSec: float) (source:IObservable<'Source>): IObservable<'Source>=
        Observable.Delay(source, TimeSpan.FromSeconds(dueTimeSec))

    /// Time shifts the observable sequence by the specified relative time duration,
    /// using the specified scheduler to run timers.
    /// The relative time intervals between the values are preserved.
    let inline delayOn (scheduler:IScheduler) (dueTime:TimeSpan) source =
        Observable.Delay(source, dueTime, scheduler)

    /// Time shifts the observable sequence to start propagating notifications at the specified absolute time.
    /// The relative time intervals between the values are preserved.
    let inline delayUntil ( source:IObservable<'Source> ) ( dueTime:DateTimeOffset ) : IObservable<'Source> =
        Observable.Delay(source, dueTime )

    /// Time shifts the observable sequence to start propagating notifications at the specified absolute time,
    /// using the specified scheduler to run timers.
    /// The relative time intervals between the values are preserved.
    let inline delayUntilOn (scheduler:IScheduler) (dueTime:DateTimeOffset) source =
        Observable.Delay(source, dueTime, scheduler)

    /// Time shifts the observable sequence based on a delay selector function for each element.
    let inline delayMap ( delayDurationSelector:'Source -> IObservable<'TDelay> )  ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.Delay( source, Func<'Source,IObservable<'TDelay>> delayDurationSelector)


    /// Time shifts the observable sequence based on a subscription delay and a delay selector function for each element.
    let inline delayMapFilter  ( delayDurationSelector         : 'Source -> IObservable<'TDelay>)
                        ( subscriptionDelay             : IObservable<'TDelay>)
                        ( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.Delay(source, subscriptionDelay, Func<'Source, IObservable<'TDelay>> delayDurationSelector)


    /// Time shifts the observable sequence by delaying the subscription with the specified relative time duration.
    let inline delaySubscription ( dueTime:TimeSpan) ( source:IObservable<'Source> ): IObservable<'Source> =
        Observable.DelaySubscription( source, dueTime )


    /// Time shifts the observable sequence by delaying the subscription with the specified relative time duration,
    /// using the specified scheduler to run timers.
    let inline delaySubscriptionOn (scheduler:IScheduler) (dueTime:TimeSpan) source =
        Observable.DelaySubscription(source, dueTime, scheduler)

    /// Time shifts the observable sequence by delaying the subscription to the specified absolute time.
    let inline delaySubscriptionUntil ( dueTime:DateTimeOffset) ( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.DelaySubscription( source, dueTime )


    /// Time shifts the observable sequence by delaying the subscription to the specified absolute time,
    /// using the specified scheduler to run timers.
    let inline delaySubscriptionUntilOn (scheduler:IScheduler) (dueTime:DateTimeOffset) source =
        Observable.DelaySubscription(source, dueTime, scheduler)

    /// Returns an observable sequence that produces a value after each period
    let inline interval period =
        Observable.Interval(period)

    /// Returns an observable sequence that produces a value on the specified scheduler after each period
    let inline intervalOn (scheduler : IScheduler) period =
        Observable.Interval(period, scheduler)

    /// Records the time interval between consecutive elements in an observable sequence.
    let inline timeInterval ( source:IObservable<'Source>) : IObservable<TimeInterval<'Source>> =
        Observable.TimeInterval( source )


    /// Records the time interval between consecutive elements in an observable sequence,
    /// using the specified scheduler to compute time intervals.
    let inline timeIntervalOn scheduler source =
        Observable.TimeInterval( source, scheduler)


    /// Applies a timeout policy to the observable sequence based on an absolute time.
    /// If the sequence doesn't terminate before the specified absolute due time, a TimeoutException is propagated to the observer.
    let inline timeout ( timeout:System.DateTimeOffset ) ( source:IObservable<'Source>) =
        Observable.Timeout( source, timeout)


    /// Applies a timeout policy to the observable sequence based on an absolute time, using the specified scheduler to run timeout timers.
    /// If the sequence doesn't terminate before the specified absolute due time, a TimeoutException is propagated to the observer.
    let inline timeoutOn (scheduler:IScheduler) (timeout:DateTimeOffset) source =
        Observable.Timeout( source, timeout, scheduler )


    /// Applies a timeout policy to the observable sequence based on an absolute time.
    /// If the sequence doesn't terminate before the specified absolute due time, the other
    /// observable sequence is used to produce future messages from that point on.
    let inline timeoutOther ( timeout:System.DateTimeOffset ) ( other:IObservable<'Source>) ( source:IObservable<'Source>) =
        Observable.Timeout( source, timeout, other)


    /// Applies a timeout policy to the observable sequence based on an absolute time,
    /// using the specified scheduler to run timeout timers.
    /// If the sequence doesn't terminate before the specified absolute due time, the other
    /// observable sequence is used to produce future messages from that point on.
    let inline timeoutOtherOn (scheduler:IScheduler) (timeout:DateTimeOffset) other source =
        Observable.Timeout( source, timeout, other, scheduler )


    /// Applies a timeout policy for each element in the observable sequence.
    /// If the next element isn't received within the specified timeout duration starting from its
    /// predecessor, a TimeoutException is propagated to the observer.
    let inline timeoutSpan ( timeout:TimeSpan ) ( source:IObservable<'Source> ) =
        Observable.Timeout( source, timeout)


    /// Applies a timeout policy for each element in the observable sequence, using the specified scheduler to run timeout timers.
    /// If the next element isn't received within the specified timeout duration starting from its
    /// predecessor, a TimeoutException is propagated to the observer.
    let inline timeoutSpanOn (scheduler:IScheduler) (timeout:TimeSpan) source =
        Observable.Timeout( source, timeout, scheduler )


    /// Applies a timeout policy for each element in the observable sequence.
    /// If the next element isn't received within the specified timeout duration starting from
    /// its predecessor, the other observable sequence is used to produce future messages from that point on.
    let inline timeoutSpanOther( timeout:TimeSpan ) ( other:IObservable<'Source> ) ( source:IObservable<'Source> ) =
        Observable.Timeout( source, timeout, other)


    /// Applies a timeout policy for each element in the observable sequence, using the specified scheduler to run timeout timers.
    /// If the next element isn't received within the specified timeout duration starting from
    /// its predecessor, the other observable sequence is used to produce future messages from that point on.
    let inline timeoutSpanOtherOn (scheduler:IScheduler) (timeout:TimeSpan) other source =
        Observable.Timeout( source, timeout, other, scheduler)


    /// Applies a timeout policy to the observable sequence based on a timeout duration computed for each element.
    /// If the next element isn't received within the computed duration starting from its predecessor,
    /// a TimeoutException is propagated to the observer.
    let inline timeoutDuration ( durationSelector )( source:IObservable<'Source> ) : IObservable<'Source> =
        Observable.Timeout( source, Func<'Source,IObservable<'Timeout>> durationSelector   )


    /// Applies a timeout policy to the observable sequence based on an initial timeout duration
    /// for the first element, and a timeout duration computed for each subsequent element.
    /// If the next element isn't received within the computed duration starting from its predecessor,
    /// a TimeoutException is propagated to the observer.
    let inline timeout2Duration ( timeout:IObservable<'Timeout> )
                            ( durationSelector              )
                            ( source:IObservable<'Source>   ) =
        Observable.Timeout( source, timeout, Func<'Source, IObservable<'Timeout>> durationSelector)



    /// Applies a timeout policy to the observable sequence based on an initial timeout duration for the first
    /// element, and a timeout duration computed for each subsequent element.
    /// If the next element isn't received within the computed duration starting from its predecessor,
    /// the other observable sequence is used to produce future messages from that point on.
    let inline timeout2DurationOther   ( timeout: IObservable<'Timeout>)
                                ( durationSelector              )
                                ( other  : IObservable<'Source> )
                                ( source : IObservable<'Source> ) =
        Observable.Timeout( source, timeout, Func<'Source, IObservable<'Timeout>> durationSelector, other)

    /// Returns an observable sequence that produces a single value at the specified absolute due time.
    let inline timer ( dueTime:DateTimeOffset ) : IObservable<int64> =
        Observable.Timer( dueTime )


    /// Returns an observable sequence that produces a single value at the specified absolute due time,
    /// using the specified scheduler to run the timer.
    let inline timerOn (scheduler:IScheduler) (dueTime:DateTimeOffset) =
        Observable.Timer( dueTime, scheduler )


    /// Returns an observable sequence that periodically produces a value starting at the specified initial absolute due time.
    let inline timerPeriod ( dueTime:DateTimeOffset) ( period:TimeSpan ) : IObservable<int64> =
        Observable.Timer( dueTime, period)


    /// Returns an observable sequence that produces a single value after the specified relative due time has elapsed.
    let inline timerSpan ( dueTime:TimeSpan ) : IObservable<int64> =
        Observable.Timer( dueTime )


    /// Returns an observable sequence that produces a single value after the specified relative due time has elapsed,
    /// using the specified scheduler to run the timer.
    let inline timerSpanOn (scheduler:IScheduler) (dueTime:TimeSpan) =
        Observable.Timer( dueTime, scheduler)


    /// Returns an observable sequence that periodically produces a value after the specified
    /// initial relative due time has elapsed.
    let inline timerSpanPeriod ( dueTime:TimeSpan, period:TimeSpan ) : IObservable<int64> =
        Observable.Timer( dueTime, period)


    /// Returns an observable sequence that periodically produces a value after the specified
    /// initial relative due time has elapsed, using the specified scheduler to run the timer.
    let inline timerSpanPeriodOn (scheduler:IScheduler) (dueTime:TimeSpan) (period:TimeSpan) =
        Observable.Timer( dueTime, period, scheduler)


    /// Timestamps each element in an observable sequence using the local system clock.
    let inline timestamp ( source:IObservable<'Source> ) : IObservable<Timestamped<'Source>> =
        Observable.Timestamp( source )

    /// Timestamps each element in an observable sequence using the supplied scheduler.
    let inline timestampOn (scheduler : IScheduler)  ( source:IObservable<'Source> ) : IObservable<Timestamped<'Source>> =
        Observable.Timestamp( source, scheduler )

    /// Ignores elements from an observable sequence which are followed by another element within a specified relative time duration.
    let inline throttle  (dueTime:TimeSpan) (source:IObservable<'Source>): IObservable<'Source> =
        Observable.Throttle( source, dueTime )

    /// Ignores elements from an observable sequence which are followed by another element within a specified relative time duration.
    let inline throttleOn (scheduler : IScheduler) (dueTime:TimeSpan) (source:IObservable<'Source>): IObservable<'Source> =
        Observable.Throttle( source, dueTime, scheduler )

    /// Ignores elements from an observable sequence which are followed by another value within a computed throttle duration
    let inline throttleComputed (throttleDurationSelector) ( source:IObservable<'Source>) : IObservable<'Source> =
        Observable.Throttle( source, Func<'Source,IObservable<'Throttle>> throttleDurationSelector )