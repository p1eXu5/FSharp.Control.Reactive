[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module FSharp.Control.Reactive.Observer

open System
open System.Reactive

/// Creates an observer from the specified onNext function.
let inline ofNext onNext =
    Observer.Create(Action<_> onNext)

/// Creates an observer from the specified onNext and onError functions.
let inline ofNextError onNext onError =
    Observer.Create(Action<_> onNext, Action<_> onError)

/// Creates an observer from the specified onNext and onCompleted functions.
let inline ofNextCompleted onNext onCompleted =
    Observer.Create(Action<_> onNext, Action onCompleted)

/// Creates an observer from the specified onNext, onError, and onCompleted functions.
let inline create<'a> onNext onError onCompleted : IObserver<'a> =
    Observer.Create(Action<_> onNext, Action<_> onError, Action onCompleted)

/// Creates an observer that ignores the incoming emits from 'OnNext', 'OnError', and 'OnCompleted'.
let inline ofIgnore<'a> () : IObserver<'a> =
    create ignore ignore ignore
