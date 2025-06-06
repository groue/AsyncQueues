/// A queue that serializes async operations, discards cancelled ones, and
/// can coalesce subsequent operations.
///
/// ## Overview
///
/// `CoalescingAsyncQueue` serialize asynchronous operations. Enqueued
/// operations run one after the other, in order, without overlapping.
///
/// Cancelled operations are eagerly discarded, without waiting for the
/// completion of previously enqueued operations. In this case, they throw
/// `CancellationError`.
///
/// An operation that runs with the [`.discardable`](<doc:CoalescingAsyncQueue/Policy/discardable>)
/// policy is cancelled if it is replaced by another operation.
///
/// For example:
///
/// ```swift
/// let queue = CoalescingAsyncQueue()
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
///
/// // This task will be cancelled if another operation is enqueued.
/// queue.addTask(policy: .discardable) {
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
/// ### Instance Methods
///
/// - ``addTask(policy:operation:)``
/// - ``perform(operation:)``
/// - ``perform(policy:operation:)``
/// - ``preconditionSerialized(_:file:line:)``
public struct CoalescingAsyncQueue: Sendable {
    /// The ids of the queues that are currently executing an operation.
    /// It is a set because the user may nest queues.
    @TaskLocal private static var currentQueueIds = Set<ID>()
    
    /// Controls the execution of an operation started by `CoalescingAsyncQueue`.
    public enum Policy: Sendable {
        /// An operation run with the `required` policy is not cancelled by
        /// subsequent operations.
        case required
        
        /// An operation run with `discardable` policy is cancelled by
        /// subsequent operations.
        case discardable
    }
    
    /// Helps `perform` return a `sending` result.
    private struct UncheckedSendable<Value>: @unchecked Sendable {
        var value: Value
    }
    
    private let primitiveQueue = PrimitiveAsyncQueue()
    private let cancellableMutex = Mutex<(@Sendable () -> Void)?>(nil)
    
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
        // First cancel eventual discardable task
        let cancel = cancellableMutex.withLock { cancel in
            defer { cancel = nil }
            return cancel
        }
        cancel?()
        
        let (start, end) = primitiveQueue.makeSemaphores()
        return try await withQueueID {
            defer { end.signal() }
            
            // Stop waiting if task is cancelled.
            try await start.waitUnlessCancelled()
            
            return try await operation()
        }
    }
    
    /// Returns the result of the given operation.
    ///
    /// The `operation` closure runs after previously enqueued operations
    /// are completed.
    ///
    /// The operation is cancelled when the current task is cancelled, or,
    /// if the policy is `.discardable`, when another operation is enqueued.
    ///
    /// If the operation is cancelled before the previously enqueued
    /// operations complete, `operation` is not executed and this method
    /// throws a `CancellationError`. Otherwise, `operation` can check
    /// `Task.isCancelled` or `Task.checkCancellation()` to
    /// detect cancellation.
    ///
    /// - Parameter policy: The coalescing policy of the operation.
    /// - Parameter operation: The operation to perform.
    /// - Returns: The result of `operation`.
    /// - Throws: The error of `operation`, or `CancellationError` if the
    ///   current task is cancelled before the previously enqueued
    ///   operations complete.
    public func perform<Success>(
        policy: Policy,
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async throws -> sending Success
    ) async throws -> sending Success {
        // When policy is .required, we could run the operation in the
        // current task. But when policy is .discardable, we need a distinct
        // task that can be independently cancelled.
        //
        // Let's avoid surprises, and always run the operation in a
        // distinct task.
        
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async throws -> sending Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        let cancellationMutex = Mutex(TaskCancellationState.notStarted)
        return try await withTaskCancellationHandler {
            let task = cancellationMutex.startTask {
                addTask(policy: policy) {
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
            cancellationMutex.cancelTask()
        }
    }
    
    /// Returns an unstructured Task that runs the given operation.
    ///
    /// The operation is cancelled when the returned task is cancelled, or,
    /// if the policy is `.discardable`, when another operation is enqueued.
    ///
    /// If the operation is cancelled before the previously enqueued
    /// operations complete, `operation` is not executed and the task
    /// throws a `CancellationError`. Otherwise, `operation` can check
    /// `Task.isCancelled` or `Task.checkCancellation()` to
    /// detect cancellation.
    ///
    /// You need to keep a reference to the task if you want to cancel it
    /// by calling the `Task.cancel()` method. Discarding your reference to
    /// the task doesn’t implicitly cancel that task, it only makes it
    /// impossible for you to explicitly cancel the task.
    ///
    /// - Parameter policy: The coalescing policy of the operation.
    /// - Parameter operation: The operation to perform.
    /// - Returns: a Task that executes `operation`.
    @discardableResult
    public func addTask<Success>(
        policy: Policy = .required,
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping () async throws -> Success
    ) -> Task<Success, any Error> {
        // Compiler does not see that operation is only called once.
        typealias SendableOperation = @Sendable () async throws -> Success
        let operation = unsafeBitCast(operation, to: SendableOperation.self)
        
        let (task, cancel) = cancellableMutex.withLock { cancel in
            let (start, end) = primitiveQueue.makeSemaphores()
            let task = Task {
                try await withQueueID {
                    defer { end.signal() }
                    
                    // Stop waiting if task is cancelled.
                    try await start.waitUnlessCancelled()
                    
                    return try await operation()
                }
            }
            
            let toCancel = cancel
            switch policy {
            case .required:
                cancel = nil
            case .discardable:
                cancel = task.cancel
            }
            return (task, toCancel)
        }
        
        cancel?()
        return task
    }
}

extension CoalescingAsyncQueue {
    /// Stops program execution if the current task is not executing an
    /// operation of this queue.
    ///
    /// - Parameters:
    ///   - message: The message to print if the assertion fails.
    ///   - file: The file name to print if the assertion fails. The default
    ///     is where this method was called.
    ///   - line: The line number to print if the assertion fails The
    ///     default is where this method was called.
    public func preconditionSerialized(
        _ message: @autoclosure () -> String = String(),
        file: StaticString = #fileID,
        line: UInt = #line
    ) {
        precondition(Self.currentQueueIds.contains(id), message(), file: file, line: line)
    }
    
    private func withQueueID<R>(
        operation: () async throws -> R,
        isolation: isolated (any Actor)? = #isolation,
        file: String = #fileID,
        line: UInt = #line
    ) async rethrows -> R {
        var ids = Self.currentQueueIds
        ids.insert(id)
        return try await Self.$currentQueueIds.withValue(
            ids,
            operation: operation,
            isolation: isolation,
            file: file,
            line: line)
    }
}

extension CoalescingAsyncQueue: Identifiable {
    /// The identity of a ``CoalescingAsyncQueue``.
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
