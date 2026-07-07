namespace p1eXu5.FSharp.Reactive.Tests.EmitsSpecs

open NUnit.Framework
open FsCheck
open p1eXu5.FSharp.Reactive
open p1eXu5.FSharp.Reactive.Testing
open p1eXu5.FSharp.Reactive.Tests

#nowarn "988"

[<TestFixture>]
type EmitTest () = 
    
    [<Test>]
    member __.``Notifications gets passed through`` () =
        let config = 
            Config.QuickThrowOnFailure.WithArbitrary [ typeof<GenTestNotification> ]

        Check.One(config,
            fun testNotifications ->
                TestScheduler.usage <| fun sch ->
                    TestScheduler.coldObservable sch testNotifications
                    |> TestScheduler.subscribeTestObserverStart sch
                    |> (fun testableObserver -> TestObserver.messages testableObserver = testNotifications)
        )

    [<Test>]
    member __.``Functor Law of Observables`` () =
        let config = 
            Config.QuickThrowOnFailure.WithArbitrary [ typeof<GenTestNotification> ]

        Check.One(config,
            fun testNotifications (f : int -> int) (g : int -> int) ->
                TestScheduler.usage <| fun sch ->
                    TestScheduler.hotObservable sch testNotifications
                    |> Observable.retry
                    |> Observable.map f
                    |> Observable.map g
                    |> TestScheduler.subscribeTestObserverStart sch
                    |> (fun testableObserver -> TestObserver.nexts testableObserver = TestNotification.mapNexts (f >> g) testNotifications)
            )

    [<Test>]
    member __.``Applicative Law of Observables`` () =
        let config = 
            Config.QuickThrowOnFailure.WithArbitrary [ typeof<GenTestNotification> ]

        Check.One(config,
            fun (testNotifications: TestNotifications<int>) (f : int -> int) ->
                TestScheduler.usage <| fun sch ->
                    TestScheduler.hotObservable sch testNotifications
                    |> Observable.retry
                    |> Observable.apply (Observable.retn f)
                    |> TestScheduler.subscribeTestObserverStart sch
                    |> (fun testableObserver -> TestObserver.nexts testableObserver = TestNotification.mapNexts f testNotifications)
        ) 


    [<Test>]
    member __.``Monadic Law of Observables`` () =
        let config = 
            Config.QuickThrowOnFailure.WithArbitrary [ typeof<GenTestNotification> ]

        Check.One(config,
            fun testNotifications (f : int -> int) ->
                TestScheduler.usage <| fun sch ->
                    TestScheduler.hotObservable sch testNotifications
                    |> Observable.retry
                    |> Observable.bind (f >> Observable.retn)
                    |> TestScheduler.subscribeTestObserverStart sch
                    |> (fun testableObserver -> TestObserver.nexts testableObserver = TestNotification.mapNexts f testNotifications)
        )


