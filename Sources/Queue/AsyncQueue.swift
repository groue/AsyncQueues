/// A queue that serializes async operations.
///
/// Usage:
///
/// ```swift
/// let queue = AsyncQueue()
///
/// // `perform(operation:)` waits until the async operation has
/// // completed. The operation is cancelled if the current task
/// // is cancelled.
/// let value = try await queue.perform {
///     try await someValue()
/// }
///
/// // `addTask` returns a new unstructured task that executes the
/// // async operation. As all unstructured tasks, it is not cancelled if
/// // the current task is cancelled.
/// queue.addTask {
///     try await doSomething()
/// }
/// ```
public final class AsyncQueue: Sendable {
    typealias Operation = @Sendable () async -> Void
    
    /// Helps `perform` handle cancellation.
    fileprivate enum TaskCancellationState {
        case notStarted
        case cancellable(@Sendable () -> Void)
        case cancelled
    }
    
    /// Helps `perform` return a `sending` result.
    private struct UncheckedSendable<Value>: @unchecked Sendable {
        var value: Value
    }
    
    private let queueContinuation: AsyncStream<Operation>.Continuation
    
    public init() {
        // Execute, in order, all operations submitted to `queueContinuation`.
        let (queueStream, queueContinuation) = AsyncStream.makeStream(of: Operation.self)
        self.queueContinuation = queueContinuation
        Task {
            for await operation in queueStream {
                await operation()
            }
        }
    }
    
    deinit {
        queueContinuation.finish()
    }
    
    /// Returns the result of the given operation.
    ///
    /// This method inherits cancellation of the current Task.
    ///
    /// - Important: If the current task is cancelled, this method still
    ///   runs the `operation` closure, after all previously enqueued
    ///   operations. See also ``performUnlessCancelled(operation:)``.
    ///
    /// - Parameter operation: An async closure.
    /// - Returns: The result of `operation`
    /// - Throws: The error of `operation`.
    public func perform<Success>(
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async throws -> sending Success
    ) async rethrows -> sending Success {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async throws -> sending Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        let stateMutex = Mutex(TaskCancellationState.notStarted)
        return try await withTaskCancellationHandler {
            // We accept a non-Sendable Success type because we discard the
            // task that runs the operation and just return the result.
            // We have to wrap the result in a Sendable type because Task
            // requires a Sendable result type. It is unchecked, but safe.
            let task = stateMutex.startTask {
                addTask {
                    let result = try await operation()
                    return UncheckedSendable(value: result)
                }
            }
            
            return try await task.value.value
        } onCancel: {
            stateMutex.cancel()
        }
    }
    
    /// Returns the result of the given operation.
    ///
    /// This method inherits cancellation of the current Task.
    ///
    /// If the current task is cancelled before the previously enqueued
    /// operations complete, `operation` is not executed and this method
    /// throws a `CancellationError`.
    ///
    /// - Parameter operation: An async closure.
    /// - Returns: The result of `operation`
    /// - Throws: The error of `operation`, or `CancellationError` if the
    ///   current task is cancelled before `operation` could start.
    public func performUnlessCancelled<Success>(
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async throws -> sending Success
    ) async throws -> sending Success {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async throws -> sending Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        let stateMutex = Mutex(TaskCancellationState.notStarted)
        return try await withTaskCancellationHandler {
            // We accept a non-Sendable Success type because we discard the
            // task that runs the operation and just return the result.
            // We have to wrap the result in a Sendable type because Task
            // requires a Sendable result type. It is unchecked, but safe.
            let task = stateMutex.startTask {
                addDiscardableTask {
                    let result = try await operation()
                    return UncheckedSendable(value: result)
                }
            }
            
            return try await task.value.value
        } onCancel: {
            stateMutex.cancel()
        }
    }
    
    /// Returns an unstructured Task that runs the given
    /// nonthrowing operation.
    ///
    /// You need to keep a reference to the task if you want to cancel it
    /// by calling the `Task.cancel()` method. Discarding your reference to
    /// the task doesn’t implicitly cancel that task, it only makes it
    /// impossible for you to explicitly cancel the task.
    ///
    /// - Parameters operation: An async closure.
    /// - Returns: a Task that executes `operation`.
    @discardableResult
    public func addTask<Success>(
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async -> Success
    ) -> Task<Success, Never> {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async -> Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        return enqueue { start, end in
            Task {
                defer { end.signal() }
                await start.wait()
                
                return await operation()
            }
        }
    }
    
    /// Returns an unstructured Task that runs the given throwing operation.
    ///
    /// You need to keep a reference to the task if you want to cancel it
    /// by calling the `Task.cancel()` method. Discarding your reference to
    /// the task doesn’t implicitly cancel that task, it only makes it
    /// impossible for you to explicitly cancel the task.
    ///
    /// - Parameter operation: An async closure.
    /// - Returns: a Task that executes `operation`.
    @discardableResult
    public func addTask<Success>(
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async throws -> Success
    ) -> Task<Success, any Error> {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async throws -> Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        return enqueue { start, end in
            Task {
                defer { end.signal() }
                await start.wait()
                
                return try await operation()
            }
        }
    }
    
    /// Returns an unstructured Task that runs the given operation.
    ///
    /// You need to keep a reference to the task if you want to cancel it
    /// by calling the `Task.cancel()` method. Discarding your reference to
    /// the task doesn’t implicitly cancel that task, it only makes it
    /// impossible for you to explicitly cancel the task.
    ///
    /// If the returned task is cancelled before the previously enqueued
    /// operations complete, `operation` is not executed and the task
    /// throws a `CancellationError`. In this case, it is
    /// considered "discarded".
    ///
    /// - Parameter operation: An async closure.
    /// - Returns: a Task that executes `operation`.
    @discardableResult
    public func addDiscardableTask<Success>(
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async throws -> Success
    ) -> Task<Success, any Error> {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async throws -> Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        return enqueue { start, end in
            Task {
                defer { end.signal() }
                
                // Stop waiting if task is cancelled.
                await start.waitUnlessCancelled()
                
                // Discard operation if task is cancelled.
                try Task.checkCancellation()
                return try await operation()
            }
        }
    }
    
    private func enqueue<Success, Failure>(
        _ makeTask: (
            _ start: Semaphore,
            _ end: Semaphore
        ) -> Task<Success, Failure>
    ) -> Task<Success, Failure> {
        // Create the task that runs `operation`. It waits for `start` and signals `end`.
        let start = Semaphore()
        let end = Semaphore()
        let addedTask = makeTask(start, end)
        
        // Enqueue a closure that signals `start` and waits for `end`
        queueContinuation.yield {
            start.signal()
            await end.wait()
        }
        
        return addedTask
    }
}

// MARK: - AsyncStream-based Semaphore

/// A "semaphore" that can be awaited and signaled only once.
fileprivate struct Semaphore {
    private let stream: AsyncStream<Never>
    private let continuation: AsyncStream<Never>.Continuation
    
    init() {
        (stream, continuation) = AsyncStream.makeStream()
    }
    
    /// Wait until semaphore is signaled.
    func wait() async {
        await Task {
            await waitUnlessCancelled()
        }.value
    }
    
    /// Wait until semaphore is signaled, or the current task is cancelled.
    func waitUnlessCancelled() async {
        for await _ in stream { }
    }
    
    /// Signal the semaphore.
    func signal() {
        continuation.finish()
    }
}

extension Mutex<AsyncQueue.TaskCancellationState> {
    func startTask<Success, Failure>(
        _ makeTask: () -> Task<Success, Failure>
    ) -> Task<Success, Failure> {
        let task: Task<Success, Failure>
        let isCancelled: Bool
        (task, isCancelled) = withLock { state in
            let task = makeTask()
            
            if case .cancelled = state {
                return (task, true)
            } else {
                state = .cancellable(task.cancel)
                return (task, false)
            }
        }
        
        if isCancelled {
            task.cancel()
        }
        
        return task
    }
    
    func cancel() {
        let cancel: (@Sendable () -> Void)?
        cancel = withLock { state in
            defer {
                state = .cancelled
            }
            if case .cancellable(let cancel) = state {
                return cancel
            } else {
                return nil
            }
        }
        
        cancel?()
    }
}
