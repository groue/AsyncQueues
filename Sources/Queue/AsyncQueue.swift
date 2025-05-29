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
    private enum TaskCancellationState {
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
    /// This method inherits cancellation of the wrapping Task.
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
            let task: Task<UncheckedSendable<Success>, any Error>
            let isCancelled: Bool
            (task, isCancelled) = stateMutex.withLock { state in
                let task = addTask {
                    let result = try await operation()
                    return UncheckedSendable(value: result)
                }
                
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
            return try await task.value.value
        } onCancel: {
            let cancel: (@Sendable () -> Void)?
            cancel = stateMutex.withLock { state in
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
    
    /// Returns an unstructured Task that runs the given
    /// nonthrowing operation.
    ///
    /// The returned task does not inherit cancellation of the wrapping Task.
    ///
    /// - Parameters operation: An async closure.
    /// - Returns: a Task that executes `operation`.
    @discardableResult
    public func addTask<Success: Sendable>(
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async -> Success
    ) -> Task<Success, Never> {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async -> Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        // Semaphore-like streams: signal is `continuation.finish()`,
        // wait is `for await _ in stream { }`
        let (startStream, startContinuation) = AsyncStream.makeStream(of: Never.self)
        let (endStream, endContinuation) = AsyncStream.makeStream(of: Never.self)
        
        // Create the task that runs `operation`. It waits for
        // `startStream` and signals `endStream`.
        let addedTask = Task<Success, Never> {
            defer { endContinuation.finish() }
            // Ignore cancellation while waiting for startContinuation.
            await Task { for await _ in startStream { } }.value
            return await operation()
        }
        
        // Enqueue a closure that signals `startStream` and
        // waits for `endStream`
        queueContinuation.yield {
            startContinuation.finish()
            for await _ in endStream { }
        }
        
        return addedTask
    }
    
    /// Returns an unstructured Task that runs the given throwing operation.
    ///
    /// The returned task does not inherit cancellation of the wrapping Task.
    ///
    /// - Parameter operation: An async closure.
    /// - Returns: a Task that executes `operation`.
    @discardableResult
    public func addTask<Success: Sendable>(
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async throws -> Success
    ) -> Task<Success, any Error> {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async throws -> Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        // Semaphore-like streams: signal is `continuation.finish()`,
        // wait is `for await _ in stream { }`
        let (startStream, startContinuation) = AsyncStream.makeStream(of: Never.self)
        let (endStream, endContinuation) = AsyncStream.makeStream(of: Never.self)
        
        // Create the task that runs `operation`. It waits for
        // `startStream` and signals `endStream`.
        let addedTask = Task<Success, any Error> {
            defer { endContinuation.finish() }
            // Ignore cancellation while waiting for startContinuation.
            await Task { for await _ in startStream { } }.value
            return try await operation()
        }
        
        // Enqueue a closure that signals `startStream` and
        // waits for `endStream`
        queueContinuation.yield {
            startContinuation.finish()
            for await _ in endStream { }
        }
        
        return addedTask
    }
}
