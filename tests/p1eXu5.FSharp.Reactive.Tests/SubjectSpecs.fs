module p1eXu5.FSharp.Reactive.Tests.SubjectSpecs

open NUnit.Framework
open FsCheck
open FsCheck.FSharp
open p1eXu5.FSharp.Reactive
open p1eXu5.FSharp.Reactive.Testing

[<Test>]
let ``Broadcast Subject broadcast to all observers`` () =
    Check.QuickThrowOnFailure <| fun (xs : int list) ->
        TestScheduler.usage <| fun sch ->
            use s = Subject.broadcast
            let observer = TestScheduler.subscribeTestObserver sch s

            Subject.onNexts xs s 
            |> Subject.onCompleted 
            |> ignore

            TestObserver.nexts observer = xs

[<Test>]
let ``Async Subject emits only last value of asnychronous operation`` () =
    Check.QuickThrowOnFailure <| fun (NonEmptyArray xs : NonEmptyArray<int>) ->
        TestScheduler.usage <| fun sch ->
            use s = Subject.async
            Subject.onNexts xs s
            |> Subject.onCompleted
            |> TestScheduler.subscribeTestObserver sch
            |> TestObserver.nexts
            |> (=) [Array.last xs]

[<Test>]
let ``Behavior Subject remembers last emited value for next observers`` () =
    Check.QuickThrowOnFailure <| fun (x : int) (y : int) ->
        TestScheduler.usage <| fun sch ->
            use s = Subject.behavior x
            let before, after = 
                TestScheduler.subscribeBeforeAfter 
                    sch s (Subject.onNext y)
            
            Subject.onCompleted s |> ignore
            (TestObserver.nexts before = [x; y]) |> Prop.label "Subscribe before 'OnNexts'"
            .&. (TestObserver.nexts after = [y]) |> Prop.label "Subscribe after 'OnNexts'"

[<Test>]
let ``Replay Subject re-emits notificatiosn to future observers`` () =
    Check.QuickThrowOnFailure <| fun (xs : int list) ->
        TestScheduler.usage <| fun sch ->
            use s = Subject.replay
            let before, after =
                TestScheduler.subscribeBeforeAfter
                    sch s (Subject.onNexts xs >> Subject.onCompleted)
        
            TestObserver.nexts before = TestObserver.nexts after
