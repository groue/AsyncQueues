/// A queue that serializes async operations.
///
/// ## Overview
///
/// `AsyncQueue` serialize asynchronous operations. Enqueued operations
/// run one after the other, in order, without overlapping.
///
/// Both throwing and non-throwing operations are supported. A cancelled
/// operation can only handle cancellation when it runs, i.e. after the
/// completion of previously enqueued operations.
///
/// For example:
///
/// ```swift
/// let queue = AsyncQueue()
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
/// ### Instance Methods
///
/// - ``addTask(operation:)->Task<Success,Never>``
/// - ``addTask(operation:)->Task<Success,Error>``
/// - ``perform(operation:)``
/// - ``preconditionSerialized(_:file:line:)``
public struct AsyncQueue: Sendable {
    @TaskLocal private static var currentQueueIds = Set<ID>()
    
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
        @_inheritActorContext @_implicitSelfCapture operation: () async throws -> Success
    ) async rethrows -> Success {
        let (start, end) = primitiveQueue.makeSemaphores()
        return try await withQueueID {
            defer { end.signal() }
            await start.wait()
            return try await operation()
        }
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
            await withQueueID {
                defer { end.signal() }
                await start.wait()
                
                return await operation()
            }
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
            try await withQueueID {
                defer { end.signal() }
                await start.wait()
                
                return try await operation()
            }
        }
    }
}

extension AsyncQueue {
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
    
    @discardableResult private func withQueueID<R>(
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

extension AsyncQueue: Identifiable {
    /// The identity of an ``AsyncQueue``.
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
