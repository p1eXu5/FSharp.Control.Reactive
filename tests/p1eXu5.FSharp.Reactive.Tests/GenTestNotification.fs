namespace p1eXu5.FSharp.Reactive.Tests

open FsCheck.FSharp
open p1eXu5.FSharp.Reactive.Testing

type GenTestNotification =
    static member GenNotifications<'a> () =
        let nexts_errors =
            Gen.frequency [
                (1, Gen.constant (fun t _ -> TestNotification.onError t (new System.Exception ())))
                (2, Gen.constant TestNotification.onNext) ]
            |> Gen.listOf

        let emits =
            Gen.constant (fun h t -> h :: t)
            <*> Gen.constant (fun t _ -> TestNotification.onCompleted t)
            <*> nexts_errors
            |> Gen.map List.rev

        let realisticMs = 100L
        let growingNumbers l =
            ArbMap.defaults
            |> ArbMap.generate<int64 * 'a>
            |> Gen.map (fun (x, y) -> (abs x) + realisticMs, y)
            |> Gen.listOfLength l
            |> Gen.map (List.sortBy fst)

        let zipGrowingNumbers es =
            growingNumbers (List.length es)
            |> Gen.map (List.zip es)

        emits
        >>= zipGrowingNumbers
        |> Gen.map (List.map (fun (f, (x, y)) -> f x y) >> TestNotifications)
        |> Arb.fromGen
