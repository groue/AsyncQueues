# Queue

**Utilities for serializing asynchronous operations**

**Requirements**: iOS 16.0+ / macOS 10.15+ • Swift 6.1+ / Xcode 16.4+

[**📖 Documentation**](https://swiftpackageindex.com/groue/Queue/documentation/queue)

---

## Context

All applications I worked on had to serialize some asynchronous jobs at some point. This is frequent in apps that deal with a shared mutable resource, such as a remote server: I'd rather make sure that all network requests that deal with a given resource have completed before I start a new one.

The Swift standard library does not provide any ready-made solution for this task. One has to build their own serialization on top of, say, `AsyncStream`:

<details><summary>Serialization with `AsyncStream`</summary>

```swift
// A program that prints 1, 2, 3, in this order.

// Setup
typealias Operation = @Sendable () async -> Void
let (queueStream, queueContinuation) = AsyncStream.makeStream(of: Operation.self)
let serialTask = Task {
    for await operation in queueStream {
        await operation()
    }
}

// Serialize operations
queueContinuation.yield { print("1") }
queueContinuation.yield { print("2") }
queueContinuation.yield { print("3") }

// Cleanup
queueContinuation.finish()
await serialTask.value
```

</details>

Such hand-made code gets more and more complicated as application needs grow:

- How to wait for an operation to complete? How to return a result from an operation?
- How to deal with both throwing and non-throwing operations?
- How to deal with cancellation? How to cancel one particular operation without cancelling other ones?
- How to deal with the cancellation of non-throwing operations, since they can not throw `CancellationError`?

## Overview

This package comes with three "queues" that serialize asynchronous operations. Enqueued operations run one after the other, in order, without overlapping.

They differ in their way to handle task cancellation:

- ``AsyncQueue`` can run both throwing and non-throwing operations. A cancelled operation can only handle cancellation when it runs, i.e. after the completion of previously enqueued operations. 

- `DiscardingAsyncQueue` eagerly discards cancelled operations, without waiting for the completion of previously enqueued operations. All operations may throw `CancellationError`.

- `CoalescingAsyncQueue` eagerly discards cancelled operations, like `DiscardingAsyncQueue`. It can also cancel "discardable" operations that are replaced by another operation. It is suitable for idempotent operations, such as operations that synchronize app data with server data.

💡 If your app does not intend to cancel operations, use `AsyncQueue`. You won't have to deal with errors for non-throwing operations.

💡 If your app has to deal with cancellation, `DiscardingAsyncQueue` helps cancelled operations complete as early as possible. In exchange, you'll have to deal with errors even for non-throwing operations.

💡 If your app runs idempotent operations that can be discarded without consequences, consider `CoalescingAsyncQueue`. 

## Usage

All queues have a similar API:

- `addTask()` returns a new top-level task, which you can await if you want:

    ```swift
    let task = queue.addTask {
        try await doSomething()
    }
    
    let result = try await task.value
    ```

- `perform()` returns the result of an async operation.

    ```swift
    let value = try await queue.perform {
        try await someValue()
    }
    ```

For example:

```swift
// Prints 1, 2, 3, in this order.
let queue = AsyncQueue()
queue.addTask { print("1") }
queue.addTask { print("2") }
await queue.perform { print("3") }
```
