namespace p1eXu5.FSharp.Reactive.Tests

open Swensen.Unquote
open p1eXu5.FSharp.Reactive.Testing

module Marbles =

/// Verifies that the given marble representation is indeed the same as the subscription found in the given test observable sequence.
    let expectSubscription txt xs =
        Marbles.subscription txt |> (=!) xs

    /// Verifies that the given marble representation is indeed the same as the subscriptions found in the given test observable sequence.
    let expectSubscriptions txt xs =
        Marbles.subscriptions txt |> (=!) xs

    /// Verifies that the given marble representation is indeed the same as the observed messages found in the given test observer.
    let expectMessages sch txt obs =
        TestScheduler.subscribeTestObserverStart sch obs
        |> TestObserver.messages =! (Marbles.parseMarbles Marbles.Cold txt)