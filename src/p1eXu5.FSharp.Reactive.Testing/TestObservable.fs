module p1eXu5.FSharp.Reactive.Testing.TestObservable

open Microsoft.Reactive.Testing

/// Gets a list of all the subscriptions to the observable sequence., including their lifetimes.
let subscriptions (o : ITestableObservable<'a>) =
    o.Subscriptions |> Seq.toList