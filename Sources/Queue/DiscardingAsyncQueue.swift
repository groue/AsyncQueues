/// A queue that serializes async operations and discards cancelled ones.
///
/// ## Overview
///
/// `DiscardingAsyncQueue` serialize asynchronous operations. Enqueued
/// operations run one after the other, in order, without overlapping.
///
/// Cancelled operations are eagerly discarded, without waiting for the
/// completion of previously enqueued operations. In this case, they throw
/// `CancellationError`.
///
/// For example:
///
/// ```swift
/// let queue = DiscardingAsyncQueue()
///
/// // `perform` returns the result of the async operation.
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
///
/// ## Topics
///
/// ### Creating a Queue
///
/// - ``init()``
///
/// ### Performing Operations
///
/// - ``perform(operation:)``
/// - ``addTask(operation:)``
public struct DiscardingAsyncQueue: Sendable {
    private let primitiveQueue = PrimitiveAsyncQueue()
    
    public init() { }
    
    /// Returns the result of the given operation.
    ///
    /// The `operation` closure runs after previously enqueued operations
    /// are completed.
    ///
    /// If the current task is cancelled before the previously enqueued
    /// operations complete, `operation` is not executed and this method
    /// throws a `CancellationError`. The operation is
    /// considered "discarded". Otherwise, `operation` can check
    /// `Task.isCancelled` or `Task.checkCancellation()` to
    /// detect cancellation.
    ///
    /// - Parameter operation: The operation to perform.
    /// - Returns: The result of `operation`.
    /// - Throws: The error of `operation`, or `CancellationError` if the
    ///   current task is cancelled before the previously enqueued
    ///   operations complete.
    public func perform<Success>(
        @_inheritActorContext @_implicitSelfCapture operation: () async throws -> Success
    ) async throws -> Success {
        let (start, end) = primitiveQueue.makeSemaphores()
        defer { end.signal() }
        
        // Stop waiting if task is cancelled.
        try await start.waitUnlessCancelled()
        
        return try await operation()
    }
    
    /// Returns an unstructured Task that runs the given operation.
    ///
    /// If the returned task is cancelled before the previously enqueued
    /// operations complete, `operation` is not executed and the task
    /// throws a `CancellationError`. The operation is
    /// considered "discarded". Otherwise, `operation` can check
    /// `Task.isCancelled` or `Task.checkCancellation()` to
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
            
            // Stop waiting if task is cancelled.
            try await start.waitUnlessCancelled()
            
            return try await operation()
        }
    }
}

extension DiscardingAsyncQueue: Identifiable {
    /// The identity of a ``DiscardingAsyncQueue``.
    public struct ID: Hashable, Sendable {
        private let id: PrimitiveAsyncQueue.ID
        
        init(id: PrimitiveAsyncQueue.ID) {
            self.id = id
        }
    }
    
    public var id: ID {
        ID(id: primitiveQueue.id)
    }
}
