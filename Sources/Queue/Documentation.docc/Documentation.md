# ``Queue``

Utilities for serializing asynchronous operations.

## Overview

This package comes with three "queues" that serialize asynchronous operations. Enqueued operations run one after the other, in order, without overlapping.

They differ in their way to handle task cancellation:

- ``AsyncQueue`` can run both throwing and non-throwing operations. A cancelled operation can only handle cancellation when it runs, i.e. after the completion of previously enqueued operations. 

- ``DiscardingAsyncQueue`` eagerly discards cancelled operations, without waiting for the completion of previously enqueued operations. All operations may throw `CancellationError`.

- ``CoalescingAsyncQueue`` eagerly discards cancelled operations, like `DiscardingAsyncQueue`. It can also cancel "discardable" operations that are replaced by another operation. It is suitable for "fire and forget" idempotent background operations.

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

## Topics

### Serial Queues

- ``AsyncQueue``
- ``DiscardingAsyncQueue``
- ``CoalescingAsyncQueue``
