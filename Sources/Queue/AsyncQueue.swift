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
public struct AsyncQueue: Sendable {
    /// Helps `perform` return a `sending` result.
    private struct UncheckedSendable<Value>: @unchecked Sendable {
        var value: Value
    }
    
    private let primitiveQueue = PrimitiveAsyncQueue()
    
    public init() { }
    
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
        
        let cancellationMutex = Mutex(TaskCancellationState.notStarted)
        return try await withTaskCancellationHandler {
            let task = cancellationMutex.startTask {
                addTask {
                    // We accept a non-Sendable Success type because we
                    // discard the task that runs the operation and just
                    // return the result.
                    //
                    // We have to wrap this result in a Sendable type
                    // because Task requires it. It is unchecked, but safe.
                    try await UncheckedSendable(value: operation())
                }
            }
            
            return try await task.value.value
        } onCancel: {
            cancellationMutex.cancel()
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
    public func addTask<Success>(
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async -> Success
    ) -> Task<Success, Never> {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async -> Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        return primitiveQueue.enqueue { start, end in
            Task {
                defer { end.signal() }
                await start.wait()
                
                return await operation()
            }
        }
    }
    
    /// Returns an unstructured Task that runs the given throwing operation.
    ///
    /// The returned task does not inherit cancellation of the wrapping Task.
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
        
        return primitiveQueue.enqueue { start, end in
            Task {
                defer { end.signal() }
                await start.wait()
                
                return try await operation()
            }
        }
    }
}
