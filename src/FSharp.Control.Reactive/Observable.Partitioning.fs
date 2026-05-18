[<AutoOpenAttribute>]
module FSharp.Control.Reactive.ObservablePartitioning

open System
open System.Reactive.Linq
open System.Reactive.Concurrency
open System.Collections.Generic

/// The Reactive module provides operators for working with IObservable<_> in F#.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Observable =

    

    /// Groups the elements of an observable sequence according to a specified key selector function.
    let inline groupBy ( keySelector  )
                ( source      : IObservable<'Source>   ) :  IObservable<IGroupedObservable<'Key,'Source>> =
        Observable.GroupBy( source,Func<'Source,'Key>  keySelector )


    /// Groups the elements of an observable sequence with the specified initial
    /// capacity according to a specified key selector function.
    //    let groupByCapacity ( keySelector )
    //                        ( capacity:int )
    //                        ( source:IObservable<'Source> ) : IObservable<IGroupedObservable<'Key,'Source>> =
    //        Observable.GroupBy( source, Func<'Source,'Key> keySelector, capacity)


    /// Groups the elements of an observable sequence according to a specified key selector function and comparer.
    let inline groupByCompare ( keySelector  )
                        ( comparer    : IEqualityComparer<_> )
                        ( source      : IObservable<_>    ) : IObservable<IGroupedObservable<_,_>> =
        Observable.GroupBy( source, Func<_,_> keySelector, comparer )


    /// Groups the elements of an observable sequence and selects the resulting elements by using a specified function.
    let inline groupByElement  ( keySelector       )
                        ( elementSelector   )
                        ( source            ) : IObservable<IGroupedObservable<'Key,'Element>> =
        Observable.GroupBy( source, Func<'Source,'Key>  keySelector, Func<'Source,'Element> elementSelector )


    /// Groups the elements of an observable sequence with the specified initial capacity
    /// according to a specified key selector function and comparer.
    //    let groupByCapacityCompare
    //                ( keySelector                           )
    //                ( capacity  : int                       )
    //                ( comparer  : IEqualityComparer<'Key>   )
    //                ( source    : IObservable<'Source>      )    : IObservable<IGroupedObservable<'Key,'Source>> =
    //        Observable.GroupBy( source, Func<'Source,'Key> keySelector, capacity, comparer )
    //

    /// Groups the elements of an observable sequence with the specified initial capacity
    /// and selects the resulting elements by using a specified function.
    let inline groupByCapacityElement
                ( keySelector           )
                ( capacity       : int  )
                ( elementSelector       )
                ( source         : IObservable<'Source> ): IObservable<IGroupedObservable<'Key,'Element>> =
        Observable.GroupBy( source, Func<'Source,'Key> keySelector, Func<'Source,'Element>  elementSelector )


    /// Groups the elements of an observable sequence according to a specified key selector function
    /// and comparer and selects the resulting elements by using a specified function.
    let inline groupByCompareElement
                ( keySelector      )
                ( comparer       : IEqualityComparer<'Key>       )
                ( elementSelector  )
                ( source         : IObservable<'Source>           ): IObservable<IGroupedObservable<'Key,'Element>> =
        Observable.GroupBy( source,Func<'Source,'Key>  keySelector, Func<'Source,'Element> elementSelector )



    /// Groups the elements of an observable sequence with the specified initial capacity according to a
    /// specified key selector function and comparer and selects the resulting elements by using a specified function.
    //    let groupByCapacityCompareElement
    //                ( keySelector      )
    //                ( capacity        : int                      )
    //                ( comparer        : IEqualityComparer<'Key> )
    //                ( elementSelector  )
    //                ( source          : IObservable<'Source>    ) : IObservable<IGroupedObservable<'Key,'Element>> =
    //        Observable.GroupBy( source, Func<'Source,'Key>    keySelector, Func<'Source,'Element> elementSelector, capacity, comparer )
    //

    ///  Groups the elements of an observable sequence according to a specified key selector function.
    ///  A duration selector function is used to control the lifetime of groups. When a group expires,
    ///  it receives an OnCompleted notification. When a new element with the same
    ///  key value as a reclaimed group occurs, the group will be reborn with a new lifetime request.
    let inline groupByUntil( keySelector )
                    ( durationSelector )
                    ( source:IObservable<'Source> ) : IObservable<IGroupedObservable<'Key,'Source>> =
        Observable.GroupByUntil( source, Func<'Source,'Key>keySelector,Func<IGroupedObservable<'Key,'Source>,IObservable<'TDuration>> durationSelector )


    /// Groups the elements of an observable sequence with the specified initial capacity according to a specified key selector function.
    /// A duration selector function is used to control the lifetime of groups. When a group
    /// expires, it receives an OnCompleted notification. When a new element with the same
    /// key value as a reclaimed group occurs, the group will be reborn with a new lifetime request.
    //    let groupByCapacityUntil
    //                    ( keySelector     )
    //                    ( capacity        : int )
    //                    ( durationSelector )
    //                    ( source          : IObservable<'Source> ): IObservable<IGroupedObservable<'Key,'Source>> =
    //        Observable.GroupByUntil( source, Func<'Source,'Key> keySelector,Func<IGroupedObservable<'Key,'Source>,IObservable<'TDuration>>  durationSelector, capacity)
    //

    /// Groups the elements of an observable sequence according to a specified key selector function and comparer.
    /// A duration selector function is used to control the lifetime of groups. When a group expires,
    /// it receives an OnCompleted notification. When a new element with the same
    /// key value as a reclaimed group occurs, the group will be reborn with a new lifetime request.
    let inline groupByComparerUntil
                    ( keySelector)
                    ( comparer: IEqualityComparer<'Key> )
                    ( durationSelector )
                    ( source:IObservable<'Source> ) : IObservable<IGroupedObservable<'Key,'Source>> =
        Observable.GroupByUntil( source, Func<'Source,'Key>  keySelector, Func<IGroupedObservable<'Key,'Source>,IObservable<'TDuration>> durationSelector, comparer )


    /// Groups the elements of an observable sequence according to a specified key selector function
    /// and selects the resulting elements by using a specified function.
    /// A duration selector function is used to control the lifetime of groups. When a group expires,
    /// it receives an OnCompleted notification. When a new element with the same
    /// key value as a reclaimed group occurs, the group will be reborn with a new lifetime request.
    let inline groupByElementUntil
                    ( keySelector )
                    ( elementSelector )
                    ( durationSelector)
                    ( source:IObservable<'Source> ): IObservable<IGroupedObservable<'Key,'Element>> =
        Observable.GroupByUntil( source, Func<'Source,'Key> keySelector, Func<'Source,'Element>elementSelector, Func<IGroupedObservable<'Key,'Element>,IObservable<'TDuration>>  durationSelector )


    /// Groups the elements of an observable sequence with the specified initial capacity according to a specified key selector function and comparer.
    /// A duration selector function is used to control the lifetime of groups. When a group expires, it receives an OnCompleted notification. When a new element with the same
    /// key value as a reclaimed group occurs, the group will be reborn with a new lifetime request.
    //    let groupByCapacityComparerUntil
    //                        ( keySelector      )
    //                        ( durationSelector )
    //                        ( capacity : int   )
    //                        ( comparer : IEqualityComparer<'Key> )
    //                        ( source   : IObservable<'Source>    ) : IObservable<IGroupedObservable<'Key,'Source>> =
    //        Observable.GroupByUntil(    source,
    //                                    Func<'Source,'Key> keySelector,
    //                                    Func<IGroupedObservable<'Key,'Source>,IObservable<'TDuration>> durationSelector,
    //                                    capacity,
    //                                    comparer                    )


    /// Groups the elements of an observable sequence with the specified initial capacity according to a specified key
    /// selector function and selects the resulting elements by using a specified function.
    /// A duration selector function is used to control the lifetime of groups. When a group
    /// expires, it receives an OnCompleted notification. When a new element with the same
    /// key value as a reclaimed group occurs, the group will be reborn with a new lifetime request.
    //    let groupByCapacityElementUntil
    //                        ( keySelector      )
    //                        ( capacity        : int )
    //                        ( elementSelector   )
    //                        ( durationSelector )
    //                        ( source          : IObservable<'Source> ) : IObservable<IGroupedObservable<'Key,'Element>> =
    //        Observable.GroupByUntil( source, Func<'Source,'Key>keySelector, Func<'Source,'Element>elementSelector, Func<IGroupedObservable<'Key,'Element>,IObservable<'TDuration>> durationSelector, capacity )


    /// Groups the elements of an observable sequence according to a specified key selector function and
    /// comparer and selects the resulting elements by using a specified function.
    /// A duration selector function is used to control the lifetime of groups. When a group expires,
    /// it receives an OnCompleted notification. When a new element with the same
    /// key value as a reclaimed group occurs, the group will be reborn with a new lifetime request.
    let inline groupByComparerElementUntil
                    ( keySelector )
                    ( comparer:Collections.Generic.IEqualityComparer<'Key>)
                    ( elementSelector )
                    ( durationSelector )
                    ( source:IObservable<'Source> ) : IObservable<IGroupedObservable<'Key,'Element>> =
        Observable.GroupByUntil( source, Func<'Source,'Key> keySelector, Func<'Source,'Element>elementSelector, Func<IGroupedObservable<'Key,'Element>,IObservable<'TDuration>>durationSelector, comparer )


    /// Groups the elements of an observable sequence with the specified initial capacity according to a specified
    /// key selector function and comparer and selects the resulting elements by using a specified function.
    /// A duration selector function is used to control the lifetime of groups. When a group expires, it receives
    /// an OnCompleted notification. When a new element with the same
    /// key value as a reclaimed group occurs, the group will be reborn with a new lifetime request.
    //    let groupByCapacityComparerElementUntil
    //                    ( keySelector )
    //                    ( capacity:int )
    //                    ( comparer:IEqualityComparer<'Key>)
    //                    ( elementSelector )
    //                    ( durationSelector )
    //                    ( source:IObservable<'Source> ) : IObservable<IGroupedObservable<'Key,'Element>> =
    //        Observable.GroupByUntil( source, Func<'Source,'Key>keySelector, Func<'Source,'Element>elementSelector, Func<IGroupedObservable<'Key,'Element>,IObservable<'TDuration>>durationSelector, capacity, comparer )

    /// Observable.Buffer(source, bufferClosingSelector)
    let inline buffer (bufferClosingSelector:IObservable<'BufferClosing>) source =
        Observable.Buffer(source, bufferClosingSelector)


    /// Projects each element of an observable sequence into
    /// consequtive non-overlapping buffers based on a sequence of boundary markers
    let inline bufferBounded (boundaries:IObservable<'BufferClosing>) source : IObservable<IList<'a>>=
        Observable.Buffer(source, boundaries)


    /// Projects each element of an observable sequence into
    /// consequtive non-overlapping buffers produced based on count information
    let inline bufferCount (count:int) source =
        Observable.Buffer(source, count)


    /// Projects each element of an observable sequence into zero or more buffers
    /// which are produced based on element count information
    let inline bufferCountSkip (count:int) (skip:int) source =
        Observable.Buffer(source,count, skip)


    /// Projects each element of an observable sequence into
    /// consequtive non-overlapping buffers produced based on timing information
    let inline bufferSpan (timeSpan:TimeSpan) source =
        Observable.Buffer(source, timeSpan)


    /// Projects each element of an observable sequence into consecutive non-overlapping buffers
    /// which are produced based on timing information, using the specified scheduler to run timers.
    let inline bufferSpanOn (scheduler:IScheduler) timeSpan source =
        Observable.Buffer (source, timeSpan, scheduler)


    /// Projects each element of an observable sequence into a buffer that goes
    /// sent out when either it's full or a specific amount of time has elapsed
    /// Analogy - A boat that departs when it's full or at its scheduled time to leave
    let inline bufferSpanCount (timeSpan:TimeSpan) (count:int) source =
        Observable.Buffer(source, timeSpan, count)


    /// Projects each element of an observable sequence into a buffer that's sent out
    /// when either it's full or a given amount of time has elapsed, using the specified scheduler to run timers.
    /// Analogy - A ferry leaves the dock when all the seats are taken, or at the scheduled time or departure,
    /// whichever event occurs first.
    let inline bufferSpanCountOn (scheduler:IScheduler) (timeSpan:TimeSpan) (count:int) source =
        Observable.Buffer(source, timeSpan, count, scheduler)


    /// Projects each element of an observable sequence into zero of more buffers.
    /// bufferOpenings - observable sequence whose elements denote the opening of each produced buffer
    /// bufferClosing - observable sequence whose elements denote the closing of each produced buffer
    let inline bufferFork  ( bufferOpenings:IObservable<'BufferOpening>)
                    ( bufferClosingSelector: 'BufferOpening ->IObservable<'a> ) source =
        Observable.Buffer( source, bufferOpenings,Func<_,_> bufferClosingSelector)


    /// Projects each element of an observable sequence into
    /// zero or more buffers produced based on timing information
    let inline bufferSpanShift (timeSpan:TimeSpan) (timeShift:TimeSpan) source =
        Observable.Buffer(source, timeSpan, timeShift)


    /// Projects each element of an observable sequence into
    /// zero or more buffers which are produced based on timing information,
    /// using the specified scheduler to run timers.
    let inline bufferSpanShiftOn (scheduler:IScheduler) (timeSpan:TimeSpan) (timeShift:TimeSpan) source =
        Observable.Buffer(source, timeSpan, timeShift, scheduler)

    /// Projects each element of an observable sequence into consecutive non-overlapping windows.
    /// windowClosingSelector - A function invoked to define the boundaries of the produced windows.
    /// A new window is started when the previous one is closed
    let inline window ( windowClosingSelector ) ( source:IObservable<'Source> ) : IObservable<IObservable<'Source>> =
        Observable.Window( source, Func<IObservable<'WindowClosing>> windowClosingSelector)


    /// Projects each element of an observable sequence into consecutive non-overlapping windows
    /// which are produced based on timing information.
    let inline windowTimeSpan ( timeSpan:TimeSpan )( source:IObservable<'Source> ) : IObservable<IObservable<'Source>> =
        Observable.Window( source, timeSpan )


    /// Projects each element of an observable sequence into consecutive non-overlapping windows
    /// which are produced based on timing information, using the specified scheduler to run timers.
    let inline windowTimeSpanOn (scheduler:IScheduler) timeSpan source =
        Observable.Window( source, timeSpan, scheduler )


    /// Projects each element of an observable sequence into zero or more windows.
    /// windowOpenings - Observable sequence whose elements denote the creation of new windows.
    /// windowClosingSelector - A function invoked to define the closing of each produced window.
    let inline windowOpenClose ( windowOpenings        : IObservable<'WinOpen>             )
                        ( windowClosingSelector : 'WinOpen->IObservable<'WinClose>  )
                        ( source                : IObservable<'Source>              ) : IObservable<IObservable<'Source>> =
        Observable.Window(source, windowOpenings, Func<_,_> windowClosingSelector)


    /// Projects each element of an observable sequence into consecutive non-overlapping windows.
    /// windowBoundaries - Sequence of window boundary markers. The current window is closed and a new window is opened upon receiving a boundary marker.
    let inline windowTimeShift ( timeSpan:TimeSpan )( timeShift:TimeSpan )( source:IObservable<'Source> ) : IObservable<IObservable<'Source>> =
        Observable.Window( source, timeSpan, timeShift )


    /// Projects each element of an observable sequence into consecutive non-overlapping windows, using the specified scheduler to run timers.
    /// windowBoundaries - Sequence of window boundary markers. The current window is closed and a new window is opened upon receiving a boundary marker.
    let inline windowTimeShiftOn (scheduler:IScheduler) (timeSpan:TimeSpan) (timeShift:TimeSpan) source =
        Observable.Window( source, timeSpan, timeShift, scheduler)


    /// Projects each element of an observable sequence into consecutive non-overlapping windows
    /// windowBoundaries - Sequence of window boundary markers. The current window is closed
    /// and a new window is opened upon receiving a boundary marker
    let inline windowBounded    ( windowBoundaries:IObservable<'WindowBoundary> )( source:IObservable<'Source> ) : IObservable<IObservable<'Source>> =
        Observable.Window( source, windowBoundaries )


    /// Projects each element of an observable sequence into zero or more windows which are produced based on element count information
    let inline windowCountSkip ( count:int )( skip:int ) ( source:IObservable<'Source> ): IObservable<IObservable<'Source>> =
        Observable.Window( source, count, skip )


    /// Projects each element of an observable sequence into consecutive non-overlapping windows
    /// which are produced based on element count information.
    let inline windowCount ( count:int )( source:IObservable<'Source> ) : IObservable<IObservable<'Source>> =
        Observable.Window( source, count )


    /// Projects each element of an observable sequence into a window that is completed when either it's full or
    /// a given amount of time has elapsed.
    /// A useful real-world analogy of this overload is the behavior of a ferry leaving the dock when all seats are
    /// taken, or at the scheduled time of departure, whichever event occurs first
    let inline windowTimeCount ( timeSpan:TimeSpan ) (count:int) ( source:IObservable<'Source> ): IObservable<IObservable<'Source>> =
        Observable.Window( source, timeSpan, count )


    /// Projects each element of an observable sequence into a window that is completed when either it's full or
    /// a given amount of time has elapsed, using the specified scheduler to run timers.
    /// A useful real-world analogy of this overload is the behavior of a ferry leaving the dock when all seats are
    /// taken, or at the scheduled time of departure, whichever event occurs first
    let inline windowTimeCountOn (scheduler:IScheduler) (timeSpan:TimeSpan) (count:int) source =
        Observable.Window( source, timeSpan, count, scheduler)

