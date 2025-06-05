/// A queue that serializes async operations.
///
/// Usage:
///
/// ```swift
/// let queue = AsyncQueue()
///
/// // `perform(operation:)` returns the result of the async operation.
/// // The operation is cancelled if the current task is cancelled.
/// let value = try await queue.perform {
///     try await someValue()
/// }
///
/// // `addTask` returns a new unstructured task that executes the
/// // async operation.
/// queue.addTask {
///     try await doSomething()
/// }
/// ```
public struct AsyncQueue: Sendable {
    private let primitiveQueue = PrimitiveAsyncQueue()
    
    public init() { }
    
    /// Returns the result of the given operation.
    ///
    /// The `operation` closure runs after previously enqueued operations
    /// are completed.
    ///
    /// If the current task is cancelled, this method still runs the
    /// `operation` closure. Check `Task.isCancelled` or
    /// `Task.checkCancellation()` to detect cancellation.
    ///
    /// - Parameter operation: The operation to perform.
    /// - Returns: The result of `operation`.
    /// - Throws: The error of `operation`.
    public func perform<Success>(
        @_inheritActorContext @_implicitSelfCapture operation: () async throws -> sending Success
    ) async rethrows -> sending Success {
        let (start, end) = primitiveQueue.makeSemaphores()
        defer { end.signal() }
        await start.wait()
        return try await operation()
    }
    
    /// Returns an unstructured Task that runs the given
    /// nonthrowing operation.
    ///
    /// The `operation` closure runs after previously enqueued operations
    /// are completed.
    ///
    /// If the returned task is cancelled, it still runs the `operation`
    /// closure. Check `Task.isCancelled` or `Task.checkCancellation()` to
    /// detect cancellation.
    ///
    /// You need to keep a reference to the task if you want to cancel it
    /// by calling the `Task.cancel()` method. Discarding your reference to
    /// the task doesn’t implicitly cancel that task, it only makes it
    /// impossible for you to explicitly cancel the task.
    ///
    /// - Parameters operation: The operation to perform.
    /// - Returns: a Task that executes `operation`.
    @discardableResult
    public func addTask<Success>(
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async -> Success
    ) -> Task<Success, Never> {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async -> Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        let (start, end) = primitiveQueue.makeSemaphores()
        return Task {
            defer { end.signal() }
            await start.wait()
            
            return await operation()
        }
    }
    
    /// Returns an unstructured Task that runs the given throwing operation.
    ///
    /// The `operation` closure runs after previously enqueued operations
    /// are completed.
    ///
    /// If the returned task is cancelled, it still runs the `operation`
    /// closure. Check `Task.isCancelled` or `Task.checkCancellation()` to
    /// detect cancellation.
    ///
    /// You need to keep a reference to the task if you want to cancel it
    /// by calling the `Task.cancel()` method. Discarding your reference to
    /// the task doesn’t implicitly cancel that task, it only makes it
    /// impossible for you to explicitly cancel the task.
    ///
    /// - Parameter operation: The operation to perform.
    /// - Returns: a Task that executes `operation`.
    @discardableResult
    public func addTask<Success>(
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async throws -> Success
    ) -> Task<Success, any Error> {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async throws -> Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        let (start, end) = primitiveQueue.makeSemaphores()
        return Task {
            defer { end.signal() }
            await start.wait()
            
            return try await operation()
        }
    }
}
